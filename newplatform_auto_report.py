"""Automation script for collecting Daily Channel Statistics from the new platform.
This script handles login (including Google Authenticator two-factor codes), loads the
Daily Channel Statistics table by scrolling the virtualized list, and exports the
records into a CSV file. Configuration is primarily driven by environment variables so
that credentials or selector adjustments never need to be hard-coded inside the script.

Key environment variables:
    NEWPLATFORM_USERNAME          The account username for the platform.
    NEWPLATFORM_PASSWORD          The account password.
    NEWPLATFORM_TOTP_SECRET       Base32 secret used to generate Google Authenticator
                                  (TOTP) codes. Optional – if omitted the script will
                                  pause for manual code entry.
    NEWPLATFORM_SAVE_DIR          Where to store the generated CSV file. Defaults to a
                                  "newplatform_reports" folder on the desktop.
    NEWPLATFORM_HEADLESS          Set to "true" to run Chrome in headless mode.
    NEWPLATFORM_LOGIN_URL         Override the login URL if the default ever changes.

Selectors can also be customised through environment variables. Provide a comma
separated list of CSS selectors or XPath expressions. The script will try them one by
one until an element is found.
    NEWPLATFORM_SELECTOR_USERNAME
    NEWPLATFORM_SELECTOR_PASSWORD
    NEWPLATFORM_SELECTOR_TOTP
    NEWPLATFORM_SELECTOR_SUBMIT
    NEWPLATFORM_SELECTOR_TABLE_CONTAINER
    NEWPLATFORM_SELECTOR_TABLE_ROWS
    NEWPLATFORM_SELECTOR_TABLE_HEADERS

Usage:
    python newplatform_auto_report.py

Make sure Google Chrome is installed on the machine. The script relies on
webdriver-manager to download a matching ChromeDriver binary automatically.
"""
from __future__ import annotations

import base64
import csv
import hmac
import hashlib
import os
import struct
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# Default configuration values -------------------------------------------------
DEFAULT_LOGIN_URL = "https://newplatform.mygamevnl.com/#/DailyChanneStatistics"
DEFAULT_SAVE_DIR = Path.home() / "Desktop" / "newplatform_reports"

# Reasonable default selectors for the login form and the statistics table.
# They can be overridden individually through environment variables.
DEFAULT_SELECTORS = {
    "username": [
        (By.CSS_SELECTOR, "input[name='username']"),
        (By.CSS_SELECTOR, "input[placeholder*='账号']"),
        (By.CSS_SELECTOR, "input[placeholder*='账户']"),
        (By.CSS_SELECTOR, "form input[type='text']"),
    ],
    "password": [
        (By.CSS_SELECTOR, "input[name='password']"),
        (By.CSS_SELECTOR, "input[type='password']"),
        (By.CSS_SELECTOR, "input[placeholder*='密码']"),
    ],
    "totp": [
        (By.CSS_SELECTOR, "input[name='googleCode']"),
        (By.CSS_SELECTOR, "input[placeholder*='验证码']"),
        (By.XPATH, "//input[contains(@placeholder, '验证码')]")
    ],
    "submit": [
        (By.CSS_SELECTOR, "button[type='submit']"),
        (By.XPATH, "//button[contains(., '登录')]"),
        (By.XPATH, "//button[contains(., 'Log in')]")
    ],
    "table_container": [
        (By.CSS_SELECTOR, ".ant-table-body"),
        (By.CSS_SELECTOR, "div[role='rowgroup']"),
    ],
    "table_rows": [
        (By.CSS_SELECTOR, ".ant-table-body table tbody tr"),
        (By.CSS_SELECTOR, "div[role='rowgroup'] div[role='row']"),
    ],
    "table_headers": [
        (By.CSS_SELECTOR, ".ant-table-body table thead th"),
        (By.CSS_SELECTOR, "table thead th"),
    ],
}

LOAD_MORE_TEXT_CANDIDATES = ("加载更多", "Load more", "下一页")


def _parse_selector_env(variable: str) -> Optional[List[Tuple[str, str]]]:
    """Parse selector overrides from environment variables."""
    raw_value = os.getenv(variable, "").strip()
    if not raw_value:
        return None

    selectors: List[Tuple[str, str]] = []
    for chunk in raw_value.split(","):
        value = chunk.strip()
        if not value:
            continue
        if value.startswith("//"):
            selectors.append((By.XPATH, value))
        else:
            selectors.append((By.CSS_SELECTOR, value))
    return selectors or None


@dataclass
class SelectorConfig:
    """Holds Selenium locator candidates for various UI elements."""

    username: Sequence[Tuple[str, str]] = field(default_factory=lambda: DEFAULT_SELECTORS["username"])
    password: Sequence[Tuple[str, str]] = field(default_factory=lambda: DEFAULT_SELECTORS["password"])
    submit: Sequence[Tuple[str, str]] = field(default_factory=lambda: DEFAULT_SELECTORS["submit"])
    totp: Sequence[Tuple[str, str]] = field(default_factory=lambda: DEFAULT_SELECTORS["totp"])
    table_container: Sequence[Tuple[str, str]] = field(default_factory=lambda: DEFAULT_SELECTORS["table_container"])
    table_rows: Sequence[Tuple[str, str]] = field(default_factory=lambda: DEFAULT_SELECTORS["table_rows"])
    table_headers: Sequence[Tuple[str, str]] = field(default_factory=lambda: DEFAULT_SELECTORS["table_headers"])

    @classmethod
    def from_environment(cls) -> "SelectorConfig":
        overrides = {}
        for field_name in (
            "username",
            "password",
            "submit",
            "totp",
            "table_container",
            "table_rows",
            "table_headers",
        ):
            parsed = _parse_selector_env(f"NEWPLATFORM_SELECTOR_{field_name.upper()}")
            if parsed:
                overrides[field_name] = parsed
        return cls(**overrides)


@dataclass
class Config:
    username: str
    password: str
    totp_secret: Optional[str]
    login_url: str = DEFAULT_LOGIN_URL
    save_dir: Path = DEFAULT_SAVE_DIR
    headless: bool = False
    selectors: SelectorConfig = field(default_factory=SelectorConfig.from_environment)

    @classmethod
    def from_environment(cls) -> "Config":
        username = os.getenv("NEWPLATFORM_USERNAME", "").strip()
        password = os.getenv("NEWPLATFORM_PASSWORD", "").strip()
        if not username or not password:
            raise RuntimeError(
                "Both NEWPLATFORM_USERNAME and NEWPLATFORM_PASSWORD must be provided "
                "as environment variables."
            )

        totp_secret = os.getenv("NEWPLATFORM_TOTP_SECRET")
        login_url = os.getenv("NEWPLATFORM_LOGIN_URL", DEFAULT_LOGIN_URL)
        save_dir = Path(os.getenv("NEWPLATFORM_SAVE_DIR", str(DEFAULT_SAVE_DIR))).expanduser()
        headless = os.getenv("NEWPLATFORM_HEADLESS", "false").strip().lower() in {"1", "true", "yes"}
        selectors = SelectorConfig.from_environment()
        return cls(
            username=username,
            password=password,
            totp_secret=totp_secret.strip() if totp_secret else None,
            login_url=login_url,
            save_dir=save_dir,
            headless=headless,
            selectors=selectors,
        )


# ----------------------------------------------------------------------------
# Helper utilities
# ----------------------------------------------------------------------------

def ensure_save_dir(path: Path) -> None:
    if not path.exists():
        print(f"[初始化] 创建数据保存目录: {path}")
        path.mkdir(parents=True, exist_ok=True)


def build_driver(headless: bool) -> webdriver.Chrome:
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    if headless:
        chrome_options.add_argument("--headless=new")

    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )

    print("[初始化] 正在准备 ChromeDriver...")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_window_size(1400, 900)
    return driver


def _normalize_totp_secret(secret: str) -> bytes:
    cleaned = secret.replace(" ", "").strip().upper()
    if not cleaned:
        raise ValueError("TOTP secret is empty")
    # Base32 strings need padding to be a multiple of 8 characters
    missing_padding = len(cleaned) % 8
    if missing_padding:
        cleaned += "=" * (8 - missing_padding)
    return base64.b32decode(cleaned, casefold=True)


def generate_totp(secret: str, digits: int = 6, interval: int = 30) -> str:
    """Generate a time-based one-time password compatible with Google Authenticator."""
    key = _normalize_totp_secret(secret)
    counter = int(time.time()) // interval
    msg = struct.pack(">Q", counter)
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    code = struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF
    otp = code % (10 ** digits)
    return f"{otp:0{digits}d}"


def _wait_for_first(wait: WebDriverWait, selectors: Sequence[Tuple[str, str]], *, clickable: bool = False):
    condition = EC.element_to_be_clickable if clickable else EC.presence_of_element_located
    last_error: Optional[Exception] = None
    for by, value in selectors:
        try:
            return wait.until(condition((by, value)))
        except TimeoutException as exc:  # pragma: no cover - defensive branch
            last_error = exc
            continue
    if last_error:
        raise last_error
    raise TimeoutException("No selectors provided")


def _try_click_button_by_text(driver: webdriver.Chrome, texts: Iterable[str]) -> bool:
    for text in texts:
        xpath = f"//button[contains(normalize-space(.), '{text}')]"
        buttons = driver.find_elements(By.XPATH, xpath)
        for button in buttons:
            if button.is_displayed() and button.is_enabled():
                driver.execute_script("arguments[0].click();", button)
                time.sleep(1.0)
                return True
    return False


def perform_login(driver: webdriver.Chrome, config: Config) -> None:
    print(f"[步骤1] 打开登录页: {config.login_url}")
    driver.get(config.login_url)

    wait = WebDriverWait(driver, 25)

    print("  > 输入账号...")
    username_input = _wait_for_first(wait, config.selectors.username)
    username_input.clear()
    username_input.send_keys(config.username)

    print("  > 输入密码...")
    password_input = _wait_for_first(wait, config.selectors.password)
    password_input.clear()
    password_input.send_keys(config.password)

    if config.totp_secret:
        try:
            totp_code = generate_totp(config.totp_secret)
            print("  > 输入谷歌验证码...")
            totp_input = _wait_for_first(wait, config.selectors.totp)
            totp_input.clear()
            totp_input.send_keys(totp_code)
        except TimeoutException:
            print("  [⚠️ 提示] 未找到谷歌验证码输入框，请确认是否开启了二次验证。")
        except Exception as exc:  # pragma: no cover - safety
            print(f"  [⚠️ 提示] 自动生成谷歌验证码失败: {exc}. 将等待手动输入。")

    print("  > 提交登录表单...")
    login_button = _wait_for_first(wait, config.selectors.submit, clickable=True)
    login_button.click()

    # 等待统计页面加载完成（通过表格容器出现判断）
    print("  > 等待统计页面加载...")
    try:
        _wait_for_first(WebDriverWait(driver, 30), config.selectors.table_container)
    except TimeoutException:
        raise RuntimeError("登录后未能在限定时间内加载统计页面。请检查账号权限或选择器配置。")
    print("[✅ 登录成功]")


def scroll_table_to_end(driver: webdriver.Chrome, selectors: SelectorConfig, pause: float = 1.2,
                        max_idle_loops: int = 4, max_scrolls: int = 60) -> int:
    """Scroll the virtualized statistics table until no new rows are loaded."""
    wait = WebDriverWait(driver, 20)
    table_container = _wait_for_first(wait, selectors.table_container)

    last_height = driver.execute_script("return arguments[0].scrollHeight", table_container)
    last_row_count = 0
    idle_loops = 0

    print("[步骤2] 开始滚动加载全部数据...")

    for scroll_index in range(1, max_scrolls + 1):
        driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight);", table_container)
        time.sleep(pause)
        _try_click_button_by_text(driver, LOAD_MORE_TEXT_CANDIDATES)

        rows = table_container.find_elements(By.CSS_SELECTOR, "tbody tr")
        if not rows:
            # 退而求其次，使用备用选择器
            rows = []
            for by, value in selectors.table_rows:
                rows = table_container.find_elements(by, value)
                if rows:
                    break

        current_row_count = len(rows)
        new_height = driver.execute_script("return arguments[0].scrollHeight", table_container)

        print(f"  > 第 {scroll_index} 次滚动: 行数 {current_row_count}, scrollHeight {new_height}")

        if current_row_count == last_row_count and new_height == last_height:
            idle_loops += 1
            if idle_loops >= max_idle_loops:
                print("  > 检测到没有更多新数据，结束滚动。")
                break
        else:
            idle_loops = 0

        last_height = new_height
        last_row_count = current_row_count

    return last_row_count


def extract_table_data(driver: webdriver.Chrome, selectors: SelectorConfig) -> pd.DataFrame:
    wait = WebDriverWait(driver, 20)
    table_container = _wait_for_first(wait, selectors.table_container)

    # 采集表头
    header_text: List[str] = []
    for by, value in selectors.table_headers:
        header_elements = table_container.find_elements(by, value)
        if header_elements:
            header_text = [element.text.strip() for element in header_elements]
            break
    if not header_text:
        raise RuntimeError("未能识别表头，请调整 NEWPLATFORM_SELECTOR_TABLE_HEADERS 变量。")

    # 采集表格行
    rows: List[List[str]] = []
    for by, value in selectors.table_rows:
        row_elements = table_container.find_elements(by, value)
        if row_elements:
            for row in row_elements:
                # 对于 div 形式的虚拟表格，使用 .find_elements(By.XPATH, "./*")
                cell_elements = row.find_elements(By.XPATH, "./*")
                if not cell_elements:
                    cell_elements = row.find_elements(By.CSS_SELECTOR, "td, div[role='gridcell']")
                row_values = [cell.text.strip() for cell in cell_elements]
                if any(row_values):
                    rows.append(row_values)
            break

    if not rows:
        raise RuntimeError("未采集到任何数据行，请确认滚动是否已经加载完全。")

    dataframe = pd.DataFrame(rows, columns=header_text[: len(rows[0])])
    return dataframe


def save_dataframe(df: pd.DataFrame, save_dir: Path) -> Path:
    ensure_save_dir(save_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = save_dir / f"daily_channel_statistics_{timestamp}.csv"
    df.to_csv(file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    return file_path


def main() -> None:
    print("=== 新平台每日渠道报表采集脚本 v1.0 ===")
    try:
        config = Config.from_environment()
    except RuntimeError as exc:
        print(f"[❌ 配置错误] {exc}")
        return

    driver: Optional[webdriver.Chrome] = None
    try:
        driver = build_driver(config.headless)
        perform_login(driver, config)
        row_count = scroll_table_to_end(driver, config.selectors)
        print(f"[信息] 已加载的行数: {row_count}")
        dataframe = extract_table_data(driver, config.selectors)
        file_path = save_dataframe(dataframe, config.save_dir)
        print("[✅ 完成] 数据已保存到:", file_path)
    except Exception as exc:  # pragma: no cover - runtime protection
        print(f"[❌ 错误] {exc}")
    finally:
        if driver:
            driver.quit()
            print("[结束] 浏览器已关闭。")


if __name__ == "__main__":
    main()
