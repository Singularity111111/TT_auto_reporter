import pandas as pd
import os
from datetime import date
import glob

# ===============================================
#                 ⭐⭐⭐ 配置区 ⭐⭐⭐
# ===============================================

# 1. 【重要】1xspin 基础数据的 Google Sheets 链接
#    程序将直接从这个网址在线读取数据
BASE_DATA_URL = "https://docs.google.com/spreadsheets/d/16uAgorJ0Rd6fo7oZCi0_G7y3wEtPxU1eU7A0EXz-rIU/export?format=csv&gid=0"

# 2. 爬虫数据所在的文件夹路径
#    默认指向您桌面上的 '1xspin_reports' 文件夹
CRAWLED_DATA_PATH = os.path.join(os.path.expanduser('~'), 'Desktop', '1xspin_reports')

# 3. 最终生成报表的路径
#    默认会保存在此脚本相同的文件夹中
FINAL_REPORT_PATH = "."  # "." 代表当前文件夹


# ===============================================


def find_latest_crawled_file(path, report_name_keyword):
    """根据关键词查找今天最新生成的爬虫数据文件"""
    today_str = date.today().strftime('%Y-%m-%d')
    # 构建搜索模式，例如: *首充登录留存*2025-10-07.csv
    search_pattern = os.path.join(path, f"*{report_name_keyword}*{today_str}.csv")
    files = glob.glob(search_pattern)
    if not files:
        print(f"[⚠️  警告] 在文件夹 '{path}' 中未找到今天的'{report_name_keyword}'文件！将跳过合并。")
        return None
    # 如果有多个，返回最新的一个（通常只有一个）
    return max(files, key=os.path.getctime)


def process_retention_data(filepath, column_mapping, date_col='日期'):
    """读取留存数据，重命名列，并设置日期为索引"""
    if filepath is None:
        return None
    print(f"  > 正在处理文件: {os.path.basename(filepath)}")
    df = pd.read_csv(filepath)
    # 重命名列
    df.rename(columns=column_mapping, inplace=True)
    # 确保日期列是字符串格式，以便合并
    df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
    # 只保留需要的列（日期 + 新列名）
    final_cols = [date_col] + list(column_mapping.values())
    return df[final_cols]


# --- 主程序开始 ---
print("--- 报表生成器 (v1.5 - 增强日期处理) ---")

try:
    # --- 步骤 1: 从网络加载基础数据 ---
    print(f"\n[步骤 1/4] 正在从网络链接加载基础数据...")
    print(f"  > 目标URL: {BASE_DATA_URL}")

    try:
        df_base = pd.read_csv(BASE_DATA_URL)
        print(f"  > 成功读取表格，检测到的列名为: {list(df_base.columns)}")

        if '日期Date' in df_base.columns:
            df_base.rename(columns={'日期Date': '日期'}, inplace=True)
            print("  > [✅ 自动修复] 已将列 '日期Date' 重命名为 '日期'。")

        if '日期' not in df_base.columns:
            raise KeyError("在从Google Sheets加载的数据中，未找到名为 '日期' 或 '日期Date' 的列。")

        #  清洗汇总行
        original_rows = len(df_base)
        df_base.dropna(subset=['日期'], inplace=True)
        dropped_rows = original_rows - len(df_base)
        if dropped_rows > 0:
            print(f"  > [✅ 自动清洗] 已忽略 {dropped_rows} 行没有日期的汇总数据。")

        # ⭐ 核心修复逻辑：增强日期转换的鲁棒性
        # 1. 尝试将 '日期' 列转换为日期格式，无法转换的错误值将变为 NaT (Not a Time)
        df_base['日期'] = pd.to_datetime(df_base['日期'], errors='coerce')

        # 2. 再次清洗，删除那些转换后变成无效日期的行
        original_rows_after_first_clean = len(df_base)
        df_base.dropna(subset=['日期'], inplace=True)
        dropped_invalid_date_rows = original_rows_after_first_clean - len(df_base)

        if dropped_invalid_date_rows > 0:
            print(f"  > [✅ 自动清洗] 已额外忽略 {dropped_invalid_date_rows} 行无效的日期格式数据。")

        # 3. 将所有有效的日期转换为统一的 'YYYY-MM-DD' 字符串格式
        df_base['日期'] = df_base['日期'].dt.strftime('%Y-%m-%d')
        print("[✅ 基础数据加载成功!]")

    except Exception as e:
        print(f"  [❌ 错误] 从URL加载或处理基础数据时失败。")
        raise e

    # --- 步骤 2: 加载并处理爬取的留存数据 ---
    print("\n[步骤 2/4] 正在加载并处理爬虫抓取的留存数据...")

    login_retention_cols = {
        '次日': '首充次日复登率_偏移', '3日': '首充三日复登率_偏移', '7日': '首充七日复登率_偏移',
        '15日': '首充十五日复登率_偏移', '30日': '首充三十日复登率_偏移'
    }
    play_retention_cols = {
        '次日': '首充次日复投率_偏移', '3日': '首充三日复投率_偏移', '7日': '首充七日复投率_偏移',
        '15日': '首充十五日复投率_偏移', '30日': '首充三十日复投率_偏移'
    }
    pay_retention_cols = {
        '次日': '首充次日复充率_偏移', '3日': '首充三日复充率_偏移', '7日': '首充七日复充率_偏移',
        '15日': '首充十五日复充率_偏移', '30日': '首充三十日复充率_偏移'
    }

    df_login = process_retention_data(find_latest_crawled_file(CRAWLED_DATA_PATH, "首充登录留存"), login_retention_cols)
    df_play = process_retention_data(find_latest_crawled_file(CRAWLED_DATA_PATH, "首充下注留存"), play_retention_cols)
    df_pay = process_retention_data(find_latest_crawled_file(CRAWLED_DATA_PATH, "首充付费留存"), pay_retention_cols)

    print("[✅ 爬虫数据处理完成!]")

    # --- 步骤 3: 合并数据 ---
    print("\n[步骤 3/4] 正在将基础数据与爬虫数据进行合并...")

    df_final = df_base
    if df_login is not None:
        df_final = pd.merge(df_final, df_login, on='日期', how='left')
    if df_play is not None:
        df_final = pd.merge(df_final, df_play, on='日期', how='left')
    if df_pay is not None:
        df_final = pd.merge(df_final, df_pay, on='日期', how='left')

    print("[✅ 数据合并成功!]")

    # --- 步骤 4: 最终计算与保存 ---
    print("\n[步骤 4/4] 正在进行最终计算并生成报表...")

    # 示例：在这里可以加入您最终需要的计算公式

    today_str = date.today().strftime('%Y-%m-%d')
    final_filename = f"final_report_{today_str}.xlsx"
    final_filepath = os.path.join(FINAL_REPORT_PATH, final_filename)

    df_final.to_excel(final_filepath, index=False)

    print("\n" + "=" * 50)
    print("🎉🎉🎉 最终报表生成成功！ 🎉🎉🎉")
    print(f"文件已保存为: {final_filename}")
    print("=" * 50)

except FileNotFoundError as e:
    print(f"\n[❌ 文件未找到错误] {e}")
    print("请确保文件名和路径配置正确。")
except Exception as e:
    print(f"\n[❌ 严重错误] 程序在运行中发生意外: {e}")

