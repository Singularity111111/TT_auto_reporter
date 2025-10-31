# -*- coding: utf-8 -*-
"""
generate_daily_agent_report.py

将同一目录下的多类 CSV/XLSX（运营/代理/平台/用户日常/留存/首充LTV/成本等）
按统一口径 清洗→对齐→合并→计算，生成"每日总代数据"（渠道/总代日级，主键：[日期, 盘口, 总代号]）。

使用：
  python generate_daily_agent_report.py --input . --output 每日总代数据_自动生成.xlsx


- 与目标表对齐的 53 列已经在配置区的 FINAL_COLUMNS 中定义，可自由调整顺序。
- "总代号"来自【运营数据】里"总代名称/渠道/channel/agent_name"字段末尾括号的纯数字（支持半角/全角），
  其它来源若无 ID，按"清洗后的总代名称"映射回填。
- 平台/盘口级指标（LTV/留存/成本等）按 [日期, 盘口] 广播到所有渠道行。
- 无对应来源时，数值默认 0（个别字段留空），以保证 53 列完整输出。
"""

import argparse, os, sys, re, glob
from datetime import datetime
from hashlib import md5
import pandas as pd

# 设置输出编码为 UTF-8（解决 Windows 控制台中文显示问题）
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# ----------------------------- 可配置区域（必要时补充） -----------------------------

# 🔧 配置：汇率（后续可调整）
EXCHANGE_RATES = {
    "巴西": 6.0,
    "墨西哥": 18.7,
    # 后续可添加其他地区
}

# 🔧 配置：指定要生成的日期
# - 设置为 None 或 "latest"：自动使用最新日期
# - 设置为具体日期字符串（如 "2025-10-28"）：只处理该日期的数据
# - 可通过命令行参数 --date 覆盖此配置
TARGET_DATE = "2025-10-30"  # 指定具体日期（推荐）

# 🔧 配置：批量生成模式（可选）
# - 设置为 True 时，生成日期区间内的多个报表
BATCH_MODE = False  # True 时启用批量生成
BATCH_START_DATE = "2025-10-20"  # 批量起始日期
BATCH_END_DATE = "2025-10-28"  # 批量结束日期

# 🔧 配置：一级首充与偏移计算口径
# - PRIMARY_FIRSTPAY_SOURCE: "all" 使用裂变类型=全部；"parent" 使用裂变类型=parent
PRIMARY_FIRSTPAY_SOURCE = "all"
# - OFFSET_MODE: "retention" 直接使用留存率；"formula" 使用留存率×(首充人数/注册人数)
OFFSET_MODE = "formula"
# - WITHDRAW_APPROX_MODE: "scale" 按比例估算首充用户提现；"zero" 不估算（首充充提差=首充金额）
WITHDRAW_APPROX_MODE = "scale"

# 🔧 配置：输出字段顺序（可调整）
# - 修改此列表可以调整最终报表的列顺序
# - 注意：必须包含所有53个字段，且字段名必须完全匹配
FINAL_COLUMNS = [
    "产品","盘口","日期","总代号","总代名称","推广部门","推广方式","消耗","展示","点击","千展成本crm","点击率",
    "注册成本","首充成本","一级首充成本","注册人数","首充人数","一级首充人数","首充转化率","首充arppu","首充roas",
    "首充当日ltv","首充当日roi","首充充提差比","当日首充金额","首充当日充提差",
    "首充次日复登率_偏移","首充三日复登率_偏移","首充七日复登率_偏移","首充十五日复登率_偏移","首充三十日复登率_偏移",
    "首充次日复投率_偏移","首充三日复投率_偏移","首充七日复投率_偏移","首充十五日复投率_偏移","首充三十日复投率_偏移",
    "首充次日复充率_偏移","首充三日复充率_偏移","首充七日复充率_偏移","首充十五日复充率_偏移","首充三十日复充率_偏移",
    "首充两日ltv_偏移","首充三日ltv_偏移","首充七日ltv_偏移","首充十五日ltv_偏移","首充三十日ltv_偏移",
    "累计roas","自然月消耗","非一级首充人数/首充人数","非一级首充人数/充值人数","充值金额","充值人数","充提差"
]

# 🔧 配置：平台→推广部门映射（V3.6新增）
# - 从文件名提取平台后，通过此映射获取推广部门
PLATFORM_TO_DEPT = {
    "OK7": "天成",
    "58": "天成", 
    "AI7": "天成",
    "98": "天成",
    "LV7": "天成",
    "OO7": "天成",
    "Bmw7": "天成",
    "bmw7": "天成",  # 小写版本
    "U777": "行远",
    "u777": "行远",
    "ONE7": "强盛",
    "one7": "强盛",
    "17SS": "天龙",
    "17ss": "天龙",
    "1xspin": "A8",
    "7sortudo": "57",
    "novabr": "T9",
    "tp7": "天龙",
    "SP1": "A8",
    "sp1": "A8",
    "pg7k": "KK",
    "5151": "大河",
    "BRL77": "A8",
    "brl77": "A8",
    "b777": "A8",
    "spin77": "A8",
    "brplay7": "A8",
    "hot77": "A8",
    "super7": "A8",
    "viva7": "A8",
    "gana7": "A8",
    "ak9": "T9",
    "samba": "T9",
    "bra1": "TTVSA8",
    "M333": "M7",
    "m333": "M7",
    "T33": "天成",
    "t33": "天成",
    "IV7": "TL",
    "iv7": "TL",
    "kq7": "TL",
    "b77": "强盛",
}

# 列名别名映射（各来源 → 标准字段名）
ALIASES = {
    "date":        ["日期","时间","date","统计日期","dt"],
    "channel":     ["渠道名称","总代名称","渠道","channel","agent_name","渠道名","agent","channel_name","代理名称"],
    "agent_id":    ["代理ID","agent_id","AgentID","总代ID"],  # v2.1: 新增代理ID列识别
    "platform":    ["盘口","平台","platform","game_platform"],
    # 指标类
    "register":    ["注册人数","新增注册","注册"],
    "active":      ["活跃人数","活跃"],
    "pay_users":   ["充值人数","付费人数"],
    "pay_amount":  ["充值金额","付费金额","金额","总充值"],
    "firstpay_u":  ["首充人数","首充人数（当日）"],
    "firstpay_a":  ["首充金额","首充金额（当日）","首充付费金额","当日首充金额"],
    "pay_active_u":["活跃充值人数"],
    "impr":        ["展示","impressions"],
    "click":       ["点击","clicks"],
    "spend":       ["消耗","cost","花费","spent"],
    "withdraw":    ["提现金额","withdrew","withdrawal"],
    # 运营类（若有）
    "bet_amt":     ["投注金额","总投注额"],
    "win_amt":     ["中奖金额","总中奖额"],
    "bet_cnt":     ["投注次数"],
    "bet_users":   ["投注人数"],
    # LTV & Retention（不同来源列名可能不同，这里只示意；脚本会按常见命名抓取）
    "ltv_d1":      ["LTV(D1)","ltv_d1","ltv1"],
    "ltv_d3":      ["LTV(D3)","ltv_d3","ltv3"],
    "ltv_d7":      ["LTV(D7)","ltv_d7","ltv7"],
    "ltv_d14":     ["LTV(D14)","ltv_d14","ltv14"],
    "ltv_d30":     ["LTV(D30)","ltv_d30","ltv30"],
    # 首充LTV（FPLTV）
    "fpltv_d1":    ["FPLTV_D1","首充当日LTV","fpltv_d1"],
    "fpltv_d2":    ["FPLTV_D2","fpltv_d2"],
    "fpltv_d3":    ["FPLTV_D3","fpltv_d3"],
    "fpltv_d7":    ["FPLTV_D7","fpltv_d7"],
    "fpltv_d15":   ["FPLTV_D15","fpltv_d15"],
    "fpltv_d30":   ["FPLTV_D30","fpltv_d30"],
    # 留存（示例：D1/D3/D7/D14/D30；不同来源文件名区分：first_login / register / first_pay）
    "ret_d1":      ["D1","留存率(D1)","ret_d1"],
    "ret_d3":      ["D3","留存率(D3)","ret_d3"],
    "ret_d7":      ["D7","留存率(D7)","ret_d7"],
    "ret_d14":     ["D14","留存率(D14)","ret_d14"],
    "ret_d30":     ["D30","留存率(D30)","ret_d30"],
}

# 代码映射（根据你们的规则图可自行补全）
TYPE_MAP   = {"111":"投放","222":"网红","333":"群发(短信等)","444":"外部合作","555":"任务量(变现)","666":"私域","OP":"运营"}
MEDIA_MAP  = {"KKK":"FB","SSS":"快手","TK":"抖音/TikTok","TTT":"Twitter","GGG":"谷歌","III":"INS","QQQ":"其他","WS":"WS","ZZZ":"群发","RRR":"bigo"}
METHOD_MAP = {"AAA":"H5","BBB":"PWA","CCC":"马甲包","DDD":"谷歌包","EEE":"APK","PPP":"iOS苹果包","FFF":"小米包"}

# 部门→盘口（你们的映射，按需补全；如果平台报表有 platform 字段就优先用平台字段）
DEPT_TO_PLATFORM = {
    # 例： "A8":"A8",
}

# ----------------------------- 工具函数 -----------------------------

def read_any_csv(path):
    """自动识别分隔符/编码读取 CSV。V3.5.4: 增强编码支持 + 容错处理"""
    # V3.5.4: 扩展编码列表，覆盖更多常见编码
    encodings = [
        "utf-8", 
        "utf-8-sig", 
        "gbk", 
        "latin-1",      # 西欧字符集
        "cp1252",       # Windows西欧
        "iso-8859-1",   # ISO西欧
        "gb2312",       # 简体中文
    ]
    
    for enc in encodings:
        try:
            # sep=None + engine='python' 可自动推断分隔符
            df = pd.read_csv(path, sep=None, engine="python", dtype=str, encoding=enc)
            return df
        except Exception:
            continue
    
    # V3.5.4: 所有编码都失败时，输出警告并返回空DataFrame（而不是崩溃）
    print(f"  ⚠️ [跳过文件] 无法读取（不支持的编码）: {os.path.basename(path)}")
    return pd.DataFrame()

def read_any_table(path):
    """根据扩展名自动读取 CSV 或 Excel（首个sheet），统一为 DataFrame[str]。V3.5.4: 增强容错"""
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv", ".txt", ".tsv"]:
        return read_any_csv(path)
    # Excel
    try:
        df = pd.read_excel(path, dtype=str)
        return df
    except Exception:
        try:
            # 再试一次不同引擎
            df = pd.read_excel(path, dtype=str, engine="openpyxl")
            return df
        except Exception as e:
            # V3.5.4: Excel读取失败时返回空DataFrame
            print(f"  ⚠️ [跳过文件] 无法读取Excel: {os.path.basename(path)} ({e})")
            return pd.DataFrame()

def is_valid_data_file(filepath, filename):
    """
    白名单：只接受有效的数据源文件（V3.5.8新增）
    
    返回True的文件：
      1. downloads/目录下的所有文件
      2. 根目录下1xspingames_开头的文件（爬虫下载）
      3. 根目录下TT-开头的文件（爬虫下载）
    
    返回False的文件（被过滤）：
      - 历史Excel文件（每日总代数据_XXX.xlsx）
      - 配置文件（sites.csv、阈值营收表.xlsx）
      - 其他非数据源文件
    """
    # 1. downloads目录下的所有文件都接受
    normalized_path = filepath.replace('\\', '/')
    if 'downloads' in normalized_path:
        return True
    
    # 2. 根目录下只接受爬虫下载的特定格式
    # 1xspingames开头的文件
    if filename.startswith('1xspingames_'):
        return True
    
    # TT-开头的文件（爬虫下载的重命名文件）
    if filename.startswith('TT-'):
        return True
    
    # 3. 其他一律拒绝（包括历史Excel、sites.csv、阈值营收表等）
    return False

def is_meaningful_df(df, required_cols=None):
    """V3.5.8: 判断DataFrame是否有意义（非空、非全NA、可选必需列存在且含非空值）"""
    if not isinstance(df, pd.DataFrame) or df.shape[1] == 0 or df.empty:
        return False
    if df.dropna(how='all').empty:
        return False
    if required_cols:
        for col in required_cols:
            if col not in df.columns:
                return False
        subset = df[required_cols]
        if subset.dropna(how='all').empty:
            return False
    return True

def consolidate_duplicate_columns(df):
    """
    V3.5.8: 合并重复列名。对同名多列，按行优先取非空值（bfill）合并为单列，移除多余列。
    返回：合并的信息字典 {列名: 合并列数量}
    """
    from collections import Counter
    name_counts = Counter(df.columns)
    dup_names = [n for n, c in name_counts.items() if c > 1]
    merged_info = {}
    if dup_names:
        print(f"  [聚合前] 检测到重复列: {dup_names}")
    for name in dup_names:
        cols = [c for c in df.columns if c == name]
        # 行内优先取非空值合并
        combined = df[cols].bfill(axis=1).iloc[:, 0]
        # 移除所有重复列
        df.drop(columns=cols, inplace=True)
        # 写回单列
        df[name] = combined
        merged_info[name] = len(cols)
    if merged_info:
        total_removed = sum(c - 1 for c in merged_info.values())
        print(f"  [重复列合并] 合并 {len(merged_info)} 个列名，共移除 {total_removed} 个重复列")
    return merged_info

def deep_clean_nonscalars(df, skip_cols=None, verbose=True):
    """
    V3.5.8: 深度清理DataFrame中的非标量单元格。
    - 对于 skip_cols 跳过
    - 如单元格包含 DataFrame/Series/list/tuple/dict/ndarray → 使用 flatten_to_scalar 展平
    - 若仍为非标量，最终转为字符串
    返回：(cleaned_by_col, offenders_after) 两个字典
    """
    import numpy as np
    def _is_nonscalar(v):
        return isinstance(v, (pd.DataFrame, pd.Series, list, tuple, dict, np.ndarray))
    def _flatten_to_scalar(value):
        """本地标量展平器：最多10层，行为与主流程中的flatten_to_scalar等价"""
        for _ in range(10):
            if isinstance(value, pd.DataFrame):
                if value.shape[0] > 0 and value.shape[1] > 0:
                    value = value.iloc[0, 0]
                    continue
                return None
            if isinstance(value, pd.Series):
                if len(value) == 0:
                    return None
                non_null = value.dropna()
                value = non_null.iloc[0] if len(non_null) > 0 else value.iloc[0]
                continue
            if isinstance(value, (list, tuple, np.ndarray)):
                if len(value) == 0:
                    return None
                seq = list(value)
                picked = None
                for item in seq:
                    if item is not None and not (isinstance(item, float) and pd.isna(item)):
                        picked = item
                        break
                value = picked if picked is not None else seq[0]
                continue
            if isinstance(value, dict):
                try:
                    value = next(iter(value.values()))
                    continue
                except StopIteration:
                    return None
            break
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, (str, bytes, int, float, bool)):
            return value
        if hasattr(np, 'isscalar') and np.isscalar(value):
            return value
        return str(value)

    skip = set(skip_cols or [])
    cleaned_by_col = {}
    for col in df.columns:
        if col in skip:
            continue
        s = df[col]
        # 只在存在非标量时进行处理，避免性能损耗
        mask = s.map(_is_nonscalar)
        cnt = int(mask.sum())
        if cnt > 0:
            # 逐单元格展平 → 非标量最终转str
            df[col] = s.map(lambda x: (x if not _is_nonscalar(x) else x))
            df[col] = df[col].map(lambda x: _flatten_to_scalar(x) if _is_nonscalar(x) else x)
            df[col] = df[col].map(lambda x: x if not _is_nonscalar(x) else str(x))
            cleaned_by_col[col] = cnt

    # 二次扫描确认，无残留非标量；如仍有，强制转为字符串
    offenders_after = {}
    for col in df.columns:
        if col in skip:
            continue
        s = df[col]
        mask = s.map(lambda v: isinstance(v, (pd.DataFrame, pd.Series, list, tuple, dict, np.ndarray)))
        cnt = int(mask.sum())
        if cnt > 0:
            df[col] = s.map(lambda v: str(v) if isinstance(v, (pd.DataFrame, pd.Series, list, tuple, dict, np.ndarray)) else v)
            offenders_after[col] = cnt

    if verbose:
        if cleaned_by_col:
            total = sum(cleaned_by_col.values())
            print(f"  [深度清理] 非标量清理计数: 总计 {total} 个单元")
            for k, v in cleaned_by_col.items():
                print(f"    - {k}: {v}")
        if offenders_after:
            print(f"  [深度清理] 仍有非标量残留(已强制转str): {list(offenders_after.items())}")
    return cleaned_by_col, offenders_after

def _parse_date_any(date_str: str):
    """尝试解析 'YYYY-MM-DD' 或 'YYYYMMDD' 为date对象，失败返回None"""
    from datetime import datetime
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except Exception:
            pass
    return None

def file_covers_date(filename: str, target_date_str: str) -> bool:
    """判断文件名是否"覆盖"目标日期（仅在指定日期场景使用）
    规则：
      - TT-...-YYYY-MM-DD.* 仅当日期==目标日期
      - *_YYYYMMDD_* 仅当日期==目标日期
      - *YYYYMMDD_YYYYMMDD* 区间包含目标日期
      - *YYYY-MM-DD_YYYY-MM-DD* 区间包含目标日期
    其他：未知则返回True（不过滤）
    """
    import re
    tgt = _parse_date_any(target_date_str)
    if not tgt:
        return True

    # 单日期（横杠）
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if m:
        d = _parse_date_any(m.group(1))
        return d == tgt if d else True

    # 单日期（紧凑）
    m = re.search(r"(\d{8})", filename)
    if m:
        d = _parse_date_any(m.group(1))
        return d == tgt if d else True

    # 区间（紧凑）
    m = re.search(r"(\d{8})_(\d{8})", filename)
    if m:
        d1 = _parse_date_any(m.group(1))
        d2 = _parse_date_any(m.group(2))
        if d1 and d2 and d1 <= tgt <= d2:
            return True
        return False

    # 区间（横杠）
    m = re.search(r"(\d{4}-\d{2}-\d{2}).*(\d{4}-\d{2}-\d{2})", filename)
    if m:
        d1 = _parse_date_any(m.group(1))
        d2 = _parse_date_any(m.group(2))
        if d1 and d2 and d1 <= tgt <= d2:
            return True
        return False

    return True

def list_input_files(root_dir, target_date: str = None):
    """递归扫描目录下所有CSV/Excel文件（V3.5.9：白名单模式，不做文件名日期过滤）"""
    files = []
    
    print(f"[扫描] 正在扫描目录: {root_dir}")
    print(f"[扫描] 使用白名单模式过滤文件（不过滤文件名日期）")
    
    # v2.1: 跳过的目录列表（避免扫描Chrome和cookies等无关目录）
    # V3.5.3: 增强跳过列表，避免扫描虚拟环境
    skip_dirs = {
        'chrome_user_data',  # Chrome浏览器数据（3000+文件）
        'cookies',           # Cookie文件
        '__pycache__',       # Python缓存
        '.git',              # Git版本控制
        'node_modules',      # Node依赖（如果有）
        '.venv',             # 虚拟环境（重要！）
        'venv',              # 虚拟环境（备用名）
        '.cursor',           # Cursor编辑器缓存
    }
    
    # v2.1: 跳过的文件名模式（历史输出文件和配置文件）
    # V3.1: 增强跳过逻辑，明确排除所有历史输出文件
    skip_file_patterns = [
        'sites.csv',         # 配置文件，不是数据源！
        '阈值营收表',        # 配置文件，公式定义表，不是数据源！
        '每日总代数据',      # 匹配所有"每日总代数据"开头的文件（除了自动生成的）
        # 乱码版本的文件名
        'æ¯æ—¥æ€»ä»£æ•°æ®',
    ]
    
    scanned_count = 0
    skipped_count = 0
    
    for r, dirs, fs in os.walk(root_dir):
        # v2.1: 过滤掉不需要扫描的子目录（直接修改dirs列表）
        original_dirs = len(dirs)
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        skipped_dirs = original_dirs - len(dirs)
        
        for f in fs:
            scanned_count += 1
            
            # 跳过 Excel 临时文件（以 ~$ 开头）
            if f.startswith('~$'):
                skipped_count += 1
                continue
                
            # 跳过隐藏文件（以 . 开头）
            if f.startswith('.'):
                skipped_count += 1
                continue
            
            # V3.5.8: 白名单模式 - 简洁高效的过滤
            # 只收集 CSV/Excel 文件
            if not f.lower().endswith((".csv", ".xlsx", ".xls")):
                continue
            
            # 检查是否是根目录
            is_root_dir = (r == os.path.abspath(root_dir))
            
            # 白名单检查：只接受有效的数据源文件
            full_path = os.path.join(r, f)
            if not is_valid_data_file(full_path, f):
                skipped_count += 1
                # 只在根目录显示被过滤的文件（避免刷屏）
                if is_root_dir:
                    print(f"  [白名单过滤] {f}")
                continue
            
            # V3.5.9: 不再按文件名日期过滤，依赖文件内容的"日期"列
            # 所有白名单文件都会被读取，后续按内容日期筛选
            
            # 通过白名单，添加到文件列表
            files.append(full_path)
    
    print(f"[扫描] 完成！扫描了 {scanned_count} 个文件，跳过 {skipped_count} 个，找到 {len(files)} 个数据文件")
    return files

def to_half_width(s):
    if not isinstance(s, str):
        s = "" if pd.isna(s) else str(s)
    return "".join(chr(ord(c)-0xFEE0) if "０" <= c <= "９" else c for c in s)

def normalize_date(v):
    if pd.isna(v) or str(v).strip()=="":
        return None
    s = to_half_width(str(v)).strip()
    # 去掉"数据汇总"
    if "数据汇总" in s:
        return None
    d = pd.to_datetime(s, errors="coerce")
    if pd.isna(d):
        return None
    return d.normalize().strftime("%Y-%m-%d")

TAIL_PATTERNS = [re.compile(r"\((\d+)\)\s*$"), re.compile(r"（(\d+)）\s*$")]

def extract_agent_id_from_tail(name):
    if not isinstance(name, str):
        return None
    s = to_half_width(name).strip()
    for pat in TAIL_PATTERNS:
        m = pat.search(s)
        if m:
            return int(m.group(1))
    return None

def strip_tail_parenthesis(name):
    if not isinstance(name, str):
        return ""
    s = to_half_width(name).strip()
    for pat in TAIL_PATTERNS:
        s = pat.sub("", s).strip()
    return s

def stable_agent_id(name):
    """为没有尾括号ID的名称生成一个稳定的正整数ID（用于主键/聚合）"""
    if not isinstance(name, str) or not name:
        return None
    # 用md5前8位转为正整数，确保同名稳定
    return int(md5(name.encode("utf-8")).hexdigest()[:8], 16)

def pick_col(df, alias_list):
    for col in alias_list:
        if col in df.columns:
            return col
    # 不区分大小写再试一次
    lower_map = {c.lower(): c for c in df.columns}
    for col in alias_list:
        lc = col.lower()
        if lc in lower_map:
            return lower_map[lc]
    return None

def parse_channel_clean(clean_name):
    # 命名格式：盘口_部门_类型码_媒介码_方式码_小组
    # 增强分隔符兼容：支持 -, 空格, 多下划线等
    s = to_half_width(str(clean_name)).strip()
    s = re.sub(r"[\-\s]+", "_", s)  # 横杠和空格转下划线
    s = re.sub(r"_+", "_", s).strip("_")  # 多下划线合并
    parts = s.split("_")
    
    platform    = parts[0] if len(parts)>0 else ""   # 第1段是"盘口"
    dept        = parts[1] if len(parts)>1 else ""
    type_code   = parts[2] if len(parts)>2 else ""
    media_code  = parts[3] if len(parts)>3 else ""
    method_code = parts[4] if len(parts)>4 else ""
    group       = parts[5] if len(parts)>5 else ""
    
    return {
        "产品": "TT产品",                      # 产品固定为"TT产品"
        "盘口_token": platform,               # 暂存：后面赋到"盘口"列
        "推广部门": dept,
        "推广方式": METHOD_MAP.get(method_code, method_code),
        "type_code": type_code,
        "type_name": TYPE_MAP.get(type_code, type_code),
        "media_code": media_code,
        "media_name": MEDIA_MAP.get(media_code, media_code),
        "method_code": method_code,
        "group_name": group
    }

def is_primary_channel(clean_name):
    """
    判定"一级"渠道（用于：一级首充人数/成本）。默认：返回 False。
    你可以根据业务把规则写在这里，例如：
      - 指定部门=AX 为一级
      - 或 指定媒介/方式为一级
    """
    return False


# ----------------------------- 推广方式判断（V3.0新增） -----------------------------

def get_promotion_method(channel_names):
    """
    从渠道名称列表判断推广方式
    channel_names: 同一代理ID下所有渠道名称列表
    返回：推广方式（如"短信"、"投放"、"短信+投放"等）
    """
    if not channel_names:
        return "投放"  # 默认
    
    # 如果是单个字符串，转为列表
    if isinstance(channel_names, str):
        channel_names = [channel_names]
    
    keywords = {
        '短信': ['dx', 'duanxin', '短信'],
        '投放': ['toufang', '投放'],
        '网红': ['wanghong', '网红'],
        '自投': ['zitou', '自投'],
        '官方': ['guanfang', '官方']
    }
    
    found_methods = set()
    for channel in channel_names:
        if not isinstance(channel, str):
            continue
        channel_lower = channel.lower()
        for method, keys in keywords.items():
            if any(k in channel_lower for k in keys):
                found_methods.add(method)
    
    if not found_methods:
        return '投放'  # 默认
    
    return '+'.join(sorted(found_methods))


# ----------------------------- LTV/留存率提取（V3.0新增） -----------------------------

def extract_ltv_value(ltv_string):
    """
    从 "11.34(110.00)" 格式中提取 LTV值 11.34
    """
    if pd.isna(ltv_string) or str(ltv_string).strip() == "" or str(ltv_string) == "0(0)":
        return 0.0
    
    s = str(ltv_string).strip()
    # 匹配格式：数字(数字)
    match = re.match(r'([-\d.]+)\(', s)
    if match:
        try:
            return float(match.group(1))
        except:
            return 0.0
    
    # 如果没有括号，直接转换
    try:
        return float(s)
    except:
        return 0.0


def extract_retention_rate(ret_string):
    """
    从 "2 (8.70%)" 或 "8.70%" 或 0.087/8.7 等格式中提取留存率，统一为百分比数值（如8.70）。
    V3.6: 修改为返回百分比形式而非小数（3.11而不是0.0311）
    """
    if pd.isna(ret_string) or str(ret_string).strip() == "":
        return 0.0
    
    s = str(ret_string).strip()
    # 1) 括号内百分比：(数字%)
    m = re.search(r"\(([-\d.]+)%\)", s)
    if m:
        try:
            return float(m.group(1))  # V3.6: 直接返回百分比数值
        except Exception:
            return 0.0
    # 2) 直接百分比："8.7%"
    if "%" in s:
        try:
            return float(s.replace('%', ''))  # V3.6: 直接返回百分比数值
        except Exception:
            return 0.0
    # 3) 纯数字：0-1 视为比例需转换，>1 视为百分数
    try:
        v = float(s)
        if v <= 1.0:
            return v * 100.0  # V3.6: 将小数转为百分比
        return v
    except Exception:
        return 0.0

# ----------------------------- 智能文件选择（V3.6新增） -----------------------------

def select_best_file_by_date_range(file_paths, file_type_name=""):
    """
    V3.6: 从多个文件中选择最佳文件
    策略：优先选择日期跨度最长且最新日期最近的文件
    
    参数：
    - file_paths: 文件路径列表
    - file_type_name: 文件类型名称（用于日志）
    
    返回：最佳文件路径，如果没有有效文件则返回None
    """
    if not file_paths:
        return None
    
    if len(file_paths) == 1:
        return file_paths[0]
    
    print(f"\n  [智能文件选择] {file_type_name}: 发现{len(file_paths)}个候选文件")
    
    best_file = None
    best_score = (-1, None, -1)  # (日期跨度, 最新日期, 行数)
    
    for fpath in file_paths:
        try:
            # 读取文件
            df = read_any_table(fpath)
            if df.empty:
                continue
            
            # 查找日期列
            date_col = pick_col(df, ALIASES["date"])
            if not date_col:
                continue
            
            # 提取日期
            dates = pd.to_datetime(df[date_col], errors='coerce').dropna()
            if len(dates) == 0:
                continue
            
            min_date = dates.min()
            max_date = dates.max()
            date_span = (max_date - min_date).days
            row_count = len(df)
            
            fname = os.path.basename(fpath)
            print(f"    {fname}: 日期跨度={date_span}天 ({min_date.date()}~{max_date.date()}), 行数={row_count}")
            
            # 评分: (日期跨度, 最新日期, 行数)
            current_score = (date_span, max_date, row_count)
            
            if current_score > best_score:
                best_score = current_score
                best_file = fpath
        
        except Exception as e:
            print(f"    ⚠️ 跳过文件 {os.path.basename(fpath)}: {e}")
            continue
    
    if best_file:
        print(f"  ✓ 选中: {os.path.basename(best_file)} (跨度={best_score[0]}天, 最新={best_score[1].date()}, 行数={best_score[2]})")
    else:
        print(f"  ⚠️ 未找到有效文件")
    
    return best_file

# ----------------------------- 文件名解析（V3.0新增） -----------------------------

def parse_filename(path):
    """
    解析新格式文件名：TT-{盘口}-{地区}-{部门}-{类型}-{日期}.csv
    返回：{"盘口": str, "地区": str, "部门": str, "类型": str, "日期": str}
    """
    filename = os.path.basename(path)
    # 去掉扩展名
    name_no_ext = os.path.splitext(filename)[0]
    
    # 按"-"分割
    parts = name_no_ext.split('-')
    
    result = {
        "盘口": None,
        "地区": None,
        "部门": None,
        "类型": None,
        "日期": None,
        "汇率": EXCHANGE_RATES.get("巴西", 6.0)  # 默认巴西汇率
    }
    
    if len(parts) >= 5:
        # TT-盘口-地区-部门-类型-日期格式
        result["盘口"] = parts[1] if len(parts) > 1 else None
        result["地区"] = parts[2] if len(parts) > 2 else None
        result["部门"] = parts[3] if len(parts) > 3 else None
        # 类型和日期可能粘在一起，如"代理报表-2025-10-29"
        if len(parts) > 4:
            result["类型"] = parts[4]
        if len(parts) > 5:
            # 日期部分：2025-10-29
            date_parts = parts[5:]
            if len(date_parts) >= 3:
                result["日期"] = f"{date_parts[0]}-{date_parts[1]}-{date_parts[2]}"
        
        # 根据地区设置汇率（从配置读取）
        if result["地区"]:
            for region, rate in EXCHANGE_RATES.items():
                if region in result["地区"]:
                    result["汇率"] = rate
                    break
    
    return result


# ----------------------------- 文件分类 -----------------------------

def classify_file(path):
    """根据文件名关键字分类来源类型。"""
    name = os.path.basename(path).lower()
    if "operation_export" in name:
        return "ops"
    if "agent_report" in name or "代理报表" in name:
        return "agent"
    if "platform_report" in name:
        return "platform"
    if "user_daily_export" in name:
        return "daily"
    if "first_paid_ltv" in name or "ltv" in name:
        return "fpltv"
    # v2.1 新增：支持新的文件命名格式
    if "user_retention_first_login" in name or "首充用户登录留存" in name or "登录留存" in name:
        return "ret_login"
    if "user_retention_register_user" in name or "注册留存" in name:
        return "ret_register"
    if "user_retention_first_pay" in name or "首充用户付费留存" in name or "付费留存" in name:
        return "ret_fpay"
    # v2.1 新增：首充用户下注留存（新类型）
    if "user_retention_first_play" in name or "首充用户下注留存" in name or "下注留存" in name:
        return "ret_play"
    if "阈值营收表" in name or "阈值" in name or "cost" in name or "ads" in name:
        return "cost"
    return "unknown"


def classify_file_smart(path):
    """
    增强版文件分类：先按文件名，再按内容识别
    适用于爬虫下载的文件（文件名不包含关键字）
    """
    # 1. 先用原有的文件名规则
    typ = classify_file(path)
    if typ != "unknown":
        return typ
    
    # 2. 如果文件名规则失败，读取文件内容判断
    try:
        df = read_any_table(path)
        if df.empty:
            return "unknown"
        
        # 转换列名为字符串便于匹配
        cols_str = " ".join([str(c) for c in df.columns])
        
        # 判断规则：
        # 代理报表：有渠道列 + (注册|活跃|充值)相关指标
        has_channel = any(keyword in cols_str for keyword in ["渠道", "总代", "代理", "agent", "channel", "总代名称"])
        has_register = any(keyword in cols_str for keyword in ["注册", "register", "新增注册"])
        has_active = any(keyword in cols_str for keyword in ["活跃", "active"])
        has_pay = any(keyword in cols_str for keyword in ["充值", "付费", "pay", "充值人数", "充值金额"])
        
        if has_channel and (has_register or has_active or has_pay):
            return "agent"
        
        # 首充LTV：包含 FPLTV 或 首充LTV 相关列
        if any(keyword in cols_str for keyword in ["FPLTV", "首充LTV", "首充ltv"]):
            return "fpltv"
        
        # 平台LTV：包含 ltv_d 系列
        if any(keyword in cols_str for keyword in ["ltv_d1", "ltv_d3", "ltv_d7", "LTV(D"]):
            return "platform"
        
        # 留存：包含 D1/D3/D7/D14/D30 留存
        if any(keyword in cols_str for keyword in ["留存", "retention", "D1", "D3", "D7", "D14", "D30"]):
            # v2.1 优化：更精确的留存类型识别
            # 优先匹配更具体的类型，避免"首充"关键字导致误判
            if "下注" in cols_str or "投注" in cols_str or "first_play" in cols_str or "play" in cols_str:
                return "ret_play"
            elif ("首充" in cols_str or "first_pay" in cols_str) and ("付费" in cols_str or "充值" in cols_str):
                return "ret_fpay"
            elif "首登" in cols_str or "登录" in cols_str or "first_login" in cols_str or "login" in cols_str:
                return "ret_login"
            elif "注册" in cols_str or "register" in cols_str:
                return "ret_register"
            else:
                # 默认归类为注册留存
                return "ret_register"
        
        # 成本/广告：包含消耗/展示/点击
        if any(keyword in cols_str for keyword in ["消耗", "展示", "点击", "spend", "cost", "impression", "click"]):
            return "cost"
        
        # 日常数据：包含首充人数/首充金额
        if any(keyword in cols_str for keyword in ["首充人数", "首充金额", "firstpay"]):
            return "daily"
        
    except Exception as e:
        print(f"  [识别警告] {os.path.basename(path)}: {e}")
    
    return "unknown"

# ----------------------------- 读取与标准化 -----------------------------

def std_ops(df):
    """标准化：运营数据（权威来源：抽取总代号；可带投注/中奖/利润）"""
    c_date    = pick_col(df, ALIASES["date"])
    c_channel = pick_col(df, ALIASES["channel"])
    
    # 如果没有渠道列，说明是平台级汇总，返回空（稍后会从其他源获取）
    if c_channel is None:
        print("  Note: Operation data has no channel column (platform-level aggregate), skipping...")
        return pd.DataFrame()
    
    out = pd.DataFrame()
    out["日期"] = df[c_date].map(normalize_date)
    out["总代名称"] = df[c_channel]
    out["总代名称_清洗"] = out["总代名称"].map(strip_tail_parenthesis)
    out["总代号"] = out["总代名称"].map(extract_agent_id_from_tail)
    # 运营指标（若有）
    c_bet_amt  = pick_col(df, ALIASES["bet_amt"])
    c_win_amt  = pick_col(df, ALIASES["win_amt"])
    c_bet_cnt  = pick_col(df, ALIASES["bet_cnt"])
    c_bet_user = pick_col(df, ALIASES["bet_users"])
    if c_bet_amt:  out["投注金额"] = pd.to_numeric(df[c_bet_amt], errors="coerce").fillna(0.0)
    if c_win_amt:  out["中奖金额"] = pd.to_numeric(df[c_win_amt], errors="coerce").fillna(0.0)
    if c_bet_cnt:  out["投注次数"] = pd.to_numeric(df[c_bet_cnt], errors="coerce").fillna(0.0)
    if c_bet_user: out["投注人数"] = pd.to_numeric(df[c_bet_user], errors="coerce").fillna(0.0)
    return out.dropna(subset=["日期","总代名称"])

def std_agent(df, name_id_map, filename=None):
    """
    标准化：代理报表（注册/活跃/充值）
    V3.0新增：
    - 保留渠道名称用于推广方式判断
    - 支持汇率换算（需要filename提取地区信息）
    - 处理"首充付费金额"字段（映射为当日首充金额）
    - 处理"充提差"字段
    """
    # V3.5.6: 如果DataFrame为空，直接返回
    if df.empty:
        return pd.DataFrame(columns=["日期","总代名称","总代名称_清洗","总代号"])
    
    c_date    = pick_col(df, ALIASES["date"])
    c_channel = pick_col(df, ALIASES["channel"])
    
    # 如果没有渠道列，跳过
    if c_channel is None:
        print("  Note: Agent data has no channel column, skipping...")
        return pd.DataFrame()
    
    out = pd.DataFrame()
    out["日期"] = df[c_date].map(normalize_date)
    out["总代名称"] = df[c_channel]
    out["总代名称_清洗"] = out["总代名称"].map(strip_tail_parenthesis)
    
    # V3.0: 保留渠道名称（原始），用于后续推广方式判断
    # 注意：这里保留的是 '渠道名称' 列的原始值
    if '渠道名称' in df.columns:
        out["渠道名称_原始"] = df['渠道名称']
    
    # v2.1: 优先使用"代理ID"列（如果存在）
    c_agent_id = pick_col(df, ALIASES["agent_id"])
    if c_agent_id:
        # 直接使用CSV文件中的代理ID列
        out["总代号"] = pd.to_numeric(df[c_agent_id], errors="coerce").astype("Int64")
        print(f"  [代理数据] 使用代理ID列: {c_agent_id}")
    elif name_id_map:
        # 使用name_id_map映射
        out["总代号"] = out["总代名称_清洗"].map(name_id_map).astype("Int64")
    else:
        # 使用stable_id函数生成总代号
        from hashlib import md5
        def stable_id_func(name):
            if pd.isna(name):
                return pd.NA
            return int(md5(str(name).encode('utf-8')).hexdigest()[:8], 16)
        out["总代号"] = out["总代名称_清洗"].map(stable_id_func).astype("Int64")
    
    # V3.0: 获取汇率（从文件名解析，引用配置区汇率）
    # V3.2: 同时提取盘口信息
    # V3.6: 从平台映射推广部门
    exchange_rate = EXCHANGE_RATES.get("巴西", 6.0)  # 默认巴西汇率
    file_platform = None
    file_dept = None
    if filename:
        file_info = parse_filename(filename)
        exchange_rate = file_info.get("汇率", EXCHANGE_RATES.get("巴西", 6.0))
        file_platform = file_info.get("盘口", None)
        # V3.6: 从平台映射推广部门
        if file_platform:
            file_dept = PLATFORM_TO_DEPT.get(file_platform, file_platform)
        print(f"  [文件解析] 盘口: {file_platform}, 推广部门: {file_dept}, 地区: {file_info.get('地区', '未知')}, 汇率: {exchange_rate}")
    
    # 基础指标
    c_reg   = pick_col(df, ALIASES["register"])
    c_act   = pick_col(df, ALIASES["active"])
    c_pu    = pick_col(df, ALIASES["pay_users"])
    c_pa    = pick_col(df, ALIASES["pay_amount"])
    c_fpu   = pick_col(df, ALIASES["firstpay_u"])
    c_fpa   = pick_col(df, ALIASES["firstpay_a"])
    c_wd    = pick_col(df, ALIASES["withdraw"])
    
    # V3.0: 支持"充提差"字段
    c_deposit_withdraw_diff = pick_col(df, ["充提差", "充值提现差"])
    
    if c_reg: out["注册人数"] = pd.to_numeric(df[c_reg], errors="coerce").fillna(0).astype("Int64")
    if c_act: out["活跃人数"] = pd.to_numeric(df[c_act], errors="coerce").fillna(0).astype("Int64")
    if c_pu:  out["充值人数"] = pd.to_numeric(df[c_pu], errors="coerce").fillna(0).astype("Int64")
    
    # V3.0: 金额字段需要汇率换算
    if c_pa:  
        out["充值金额"] = pd.to_numeric(df[c_pa], errors="coerce").fillna(0.0) / exchange_rate
    if c_fpu: 
        out["首充人数"] = pd.to_numeric(df[c_fpu], errors="coerce").fillna(0).astype("Int64")
    if c_fpa: 
        out["当日首充金额"] = pd.to_numeric(df[c_fpa], errors="coerce").fillna(0.0) / exchange_rate
    if c_wd:  
        out["提现金额"] = pd.to_numeric(df[c_wd], errors="coerce").fillna(0.0) / exchange_rate
    if c_deposit_withdraw_diff:
        out["充提差"] = pd.to_numeric(df[c_deposit_withdraw_diff], errors="coerce").fillna(0.0) / exchange_rate
    
    # V3.2: 添加盘口信息（从文件名提取）
    if file_platform:
        out["盘口"] = file_platform
    
    # V3.6: 添加推广部门信息（从平台映射）
    if file_dept:
        out["推广部门"] = file_dept
    
    return out.dropna(subset=["日期","总代名称_清洗"])

def std_platform(df):
    """标准化：平台报表（平台LTV；平台/盘口级）"""
    # V3.5.6: 如果DataFrame为空，直接返回
    if df.empty:
        return pd.DataFrame(columns=["日期","盘口"])
    
    c_date = pick_col(df, ALIASES["date"])
    c_plat = pick_col(df, ALIASES["platform"])
    out = pd.DataFrame()
    out["日期"] = df[c_date].map(normalize_date)
    out["盘口"] = df[c_plat]
    # LTV
    for std_key, aliases in [("ltv_D1","ltv_d1"),("ltv_D3","ltv_d3"),("ltv_D7","ltv_d7"),("ltv_D14","ltv_d14"),("ltv_D30","ltv_d30")]:
        col = pick_col(df, ALIASES[aliases])
        if col:
            out[std_key] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return out.dropna(subset=["日期","盘口"])

def std_daily(df, name_id_map):
    """标准化：用户日常（首充/活跃充值）"""
    # V3.5.6: 如果DataFrame为空，直接返回
    if df.empty:
        return pd.DataFrame(columns=["日期"])
    
    c_date    = pick_col(df, ALIASES["date"])
    c_channel = pick_col(df, ALIASES["channel"])
    out = pd.DataFrame()
    out["日期"] = df[c_date].map(normalize_date)
    if c_channel:
        out["总代名称"] = df[c_channel]
        out["总代名称_清洗"] = out["总代名称"].map(strip_tail_parenthesis)
        # v2.1: 优先使用"代理ID"列
        c_agent_id = pick_col(df, ALIASES["agent_id"])
        if c_agent_id:
            out["总代号"] = pd.to_numeric(df[c_agent_id], errors="coerce").astype("Int64")
        else:
            out["总代号"] = out["总代名称_清洗"].map(name_id_map).astype("Int64")
    # 指标
    for k, std_name in [("firstpay_u","首充人数"),("firstpay_a","当日首充金额"),("pay_active_u","活跃充值人数")]:
        col = pick_col(df, ALIASES[k])
        if col:
            if "人数" in std_name:
                out[std_name] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int64")
            else:
                out[std_name] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return out.dropna(subset=["日期"])

def extract_primary_firstpay(df):
    """
    V3.0新增：从留存数据中提取一级首充人数
    一级首充人数 = 裂变类型为"parent"的首充人数
    """
    c_date = pick_col(df, ALIASES["date"])
    c_agent_id = pick_col(df, ALIASES["agent_id"])
    c_channel = pick_col(df, ALIASES["channel"])
    
    if c_date is None:
        return pd.DataFrame()
    
    # 过滤裂变类型
    if "裂变类型" in df.columns:
        target_type = "全部" if PRIMARY_FIRSTPAY_SOURCE == "all" else "parent"
        df = df[df["裂变类型"] == target_type].copy()
        print(f"  [一级首充] 过滤裂变类型={target_type}，剩余 {len(df)} 行")
    else:
        return pd.DataFrame()
    
    out = pd.DataFrame()
    out["日期"] = df[c_date].map(normalize_date)
    
    # 提取代理ID
    if c_agent_id:
        out["总代号"] = pd.to_numeric(df[c_agent_id], errors="coerce").astype("Int64")
    elif c_channel:
        out["总代名称"] = df[c_channel]
        out["总代名称_清洗"] = out["总代名称"].map(strip_tail_parenthesis)
        out["总代号"] = out["总代名称"].map(extract_agent_id_from_tail)
    
    # 提取首充人数作为一级首充人数
    c_firstpay = pick_col(df, ["首充人数", "firstpay_u"])
    if c_firstpay:
        out["一级首充人数"] = pd.to_numeric(df[c_firstpay], errors="coerce").fillna(0).astype("Int64")
    
    return out.dropna(subset=["日期"])


def std_retention(df, which="login", filename=None):
    """
    标准化：留存（平台/盘口级为主；如果来源有渠道列，可扩展）
    V3.0新增：
    - 过滤裂变类型="全部"
    - 解析留存率百分比格式："2 (8.70%)" → 0.087
    - 返回留存数据，同时可提取一级首充人数（需单独调用extract_primary_firstpay）
    V3.2新增：
    - 从文件名提取盘口信息
    """
    # V3.5.6: 如果DataFrame为空，直接返回
    if df.empty:
        return pd.DataFrame(columns=["日期"])
    
    c_date = pick_col(df, ALIASES["date"])
    c_plat = pick_col(df, ALIASES["platform"])
    c_channel = pick_col(df, ALIASES["channel"])
    
    # V3.2: 从文件名提取盘口
    file_platform = None
    if filename:
        file_info = parse_filename(filename)
        file_platform = file_info.get("盘口", None)
    
    # v2.1: 如果找不到日期列，返回空DataFrame
    if c_date is None:
        print(f"  [警告] 留存数据缺少日期列，列名: {df.columns.tolist()[:10]}")
        return pd.DataFrame()
    
    # V3.0: 过滤裂变类型="全部"
    if "裂变类型" in df.columns:
        df = df[df["裂变类型"] == "全部"].copy()
        print(f"  [留存数据] 过滤裂变类型=全部，剩余 {len(df)} 行")
    
    out = pd.DataFrame()
    out["日期"] = df[c_date].map(normalize_date)
    
    # If has channel column, use it
    if c_channel:
        out["总代名称"] = df[c_channel]
        out["总代名称_清洗"] = out["总代名称"].map(strip_tail_parenthesis)
        # v2.1: 优先使用"代理ID"列
        c_agent_id = pick_col(df, ALIASES["agent_id"])
        if c_agent_id:
            out["总代号"] = pd.to_numeric(df[c_agent_id], errors="coerce").astype("Int64")
        else:
            out["总代号"] = out["总代名称"].map(extract_agent_id_from_tail)
    
    # V3.2: 优先使用文件名中的盘口信息
    if file_platform:
        out["盘口"] = file_platform
    elif c_plat:
        out["盘口"] = df[c_plat]
    
    # V3.0: 解析留存率（支持特殊格式）
    # 查找留存率列：2日留存、3日留存、7日留存、15日留存、30日留存
    retention_mapping = {
        1: ["2日留存", "D1", "留存率(D1)"],
        3: ["3日留存", "D3", "留存率(D3)"],
        7: ["7日留存", "D7", "留存率(D7)"],
        15: ["15日留存", "D15", "留存率(D15)"],
        30: ["30日留存", "D30", "留存率(D30)"]
    }
    
    for days, col_names in retention_mapping.items():
        col = pick_col(df, col_names)
        if col:
            # 使用extract_retention_rate函数解析
            out[f"{which}_D{days}"] = df[col].map(extract_retention_rate)
    
    return out.dropna(subset=["日期"])

def std_fpltv(df, filename=None):
    """
    标准化：首充LTV（按代理ID）
    V3.0新增：
    - 支持新格式："第1天"、"第2天"...列
    - 解析LTV特殊格式："11.34(110.00)" → 11.34
    V3.2新增：
    - 从文件名提取盘口信息
    """
    # V3.5.6: 如果DataFrame为空，直接返回
    if df.empty:
        return pd.DataFrame(columns=["日期"])
    
    c_date = pick_col(df, ALIASES["date"])
    c_plat = pick_col(df, ALIASES["platform"])
    c_agent_id = pick_col(df, ALIASES["agent_id"])
    c_channel = pick_col(df, ALIASES["channel"])
    
    # V3.2: 从文件名提取盘口
    file_platform = None
    if filename:
        file_info = parse_filename(filename)
        file_platform = file_info.get("盘口", None)
    
    out = pd.DataFrame()
    out["日期"] = df[c_date].map(normalize_date)
    
    # V3.0: 支持代理ID维度
    if c_agent_id:
        out["总代号"] = pd.to_numeric(df[c_agent_id], errors="coerce").astype("Int64")
    
    if c_channel:
        out["总代名称"] = df[c_channel]
        out["总代名称_清洗"] = out["总代名称"].map(strip_tail_parenthesis)
    
    # V3.2: 优先使用文件名中的盘口信息
    if file_platform:
        out["盘口"] = file_platform
    elif c_plat:
        out["盘口"] = df[c_plat]
    
    # V3.0: 解析新格式LTV数据（"第X天"列）
    # V3.5.10: 支持"首充/考核N"格式（用户实际文件格式）
    # V3.5.11: 添加D14支持，兼容不同站点口径
    day_columns_mapping = {
        "FPLTV_D1": ["首充", "考核1", "第1天", "FPLTV_D1", "fpltv_d1", "D1"],
        "FPLTV_D2": ["考核2", "第2天", "FPLTV_D2", "fpltv_d2", "D2"],
        "FPLTV_D3": ["考核3", "第3天", "FPLTV_D3", "fpltv_d3", "D3"],
        "FPLTV_D7": ["考核7", "第7天", "FPLTV_D7", "fpltv_d7", "D7"],
        "FPLTV_D14": ["考核14", "第14天", "FPLTV_D14", "fpltv_d14", "D14"],
        "FPLTV_D15": ["考核15", "第15天", "FPLTV_D15", "fpltv_d15", "D15"],
        "FPLTV_D30": ["考核30", "第30天", "FPLTV_D30", "fpltv_d30", "D30"]
    }
    
    for key, col_names in day_columns_mapping.items():
        col = pick_col(df, col_names)
        if col:
            # 使用extract_ltv_value函数解析
            out[key] = df[col].map(extract_ltv_value)
    
    return out.dropna(subset=["日期"])

def std_cost(df):
    """标准化：成本/广告数据（消耗/展示/点击/提现等），可从阈值营收表或广告CSV读取。平台或渠道级都支持。"""
    c_date = pick_col(df, ALIASES["date"])
    
    # V3.5.4: 如果没有日期列，说明是配置文件（如阈值营收表），返回空DataFrame
    if c_date is None:
        print(f"  [跳过] Cost数据缺少日期列，可能是配置文件")
        return pd.DataFrame()
    
    out = pd.DataFrame()
    out["日期"] = df[c_date].map(normalize_date)
    c_channel = pick_col(df, ALIASES["channel"])
    c_plat    = pick_col(df, ALIASES["platform"])
    if c_channel:
        out["总代名称"] = df[c_channel]
        out["总代名称_清洗"] = out["总代名称"].map(strip_tail_parenthesis)
    if c_plat:
        out["盘口"] = df[c_plat]
    for k, std in [("spend","消耗"),("impr","展示"),("click","点击"),("withdraw","提现金额")]:
        col = pick_col(df, ALIASES[k])
        if col:
            out[std] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return out.dropna(subset=["日期"])

# ----------------------------- 主流程 -----------------------------

def main(input_dir, output_path, target_date=None):
    """
    生成每日总代数据报表
    
    参数:
        input_dir: 输入目录路径
        output_path: 输出文件路径
        target_date: 目标日期（字符串，如 "2025-10-27"）
                    - None 或 "latest": 自动使用最新日期
                    - 具体日期: 只处理该日期的数据
    """
    # V3.5.3: 优化扫描策略 - 只扫描downloads目录和根目录单独文件
    files = []
    downloads_path = os.path.join(input_dir, "downloads")
    if os.path.exists(downloads_path):
        print(f"[扫描] downloads目录...")
        files.extend(list_input_files(downloads_path, target_date))
    
    # 扫描根目录的单独文件（如运营报表.xlsx）
    # V3.5.8: 根目录也必须走白名单检查
    # V3.5.9: 不再按文件名日期过滤
    print(f"[扫描] 根目录单独文件...")
    try:
        for f in os.listdir(input_dir):
            full_path = os.path.join(input_dir, f)
            if os.path.isfile(full_path) and f.lower().endswith(('.csv', '.xlsx', '.xls')):
                # V3.5.8: 必须通过白名单检查
                if is_valid_data_file(full_path, f):
                    # V3.5.9: 不再按文件名日期过滤，依赖文件内容
                    files.append(full_path)
                else:
                    print(f"  [白名单过滤] {f}")
    except Exception as e:
        print(f"  [警告] 扫描根目录时出错: {e}")
    
    if not files:
        print(f"[ERROR] 输入目录 {input_dir} 下没有任何数据文件，跳过生成。")
        return
    print(f"[INFO] 找到 {len(files)} 个数据文件")
    
    # 1) 读取运营数据，构建 name_id_map
    ops_list = []
    for p in files:
        if classify_file_smart(p)=="ops":
            df = read_any_table(p)
            result = std_ops(df)
            if not result.empty:
                ops_list.append(result)
    
    if not ops_list:
        print("  Warning: No operation data with channel column found, cannot extract agent IDs from names.")
        name_id_map = {}
    else:
        ops = pd.concat(ops_list, ignore_index=True).dropna(subset=["日期","总代名称"])
        # name -> id（出现次数最多的ID）
        tmp = ops[["总代名称","总代名称_清洗","总代号"]].dropna(subset=["总代名称_清洗","总代号"])
        if tmp.empty:
            name_id_map = {}
        else:
            mode_id = (tmp.groupby("总代名称_清洗")["总代号"]
                         .agg(lambda s: s.value_counts().index[0]))
            name_id_map = mode_id.to_dict()

    # 2) V3.6: 先对文件进行分类和分组
    print("\n[分类文件]")
    file_groups = {
        "agent": [], "platform": [], "daily": [], "ops": [],
        "ret_login": [], "ret_register": [], "ret_fpay": [], "ret_play": [],
        "fpltv": [], "cost": [], "unknown": []
    }
    
    for p in files:
        typ = classify_file_smart(p)
        if typ in file_groups:
            file_groups[typ].append(p)
        else:
            file_groups["unknown"].append(p)
    
    # 打印分类统计
    for typ, paths in file_groups.items():
        if paths:
            print(f"  {typ}: {len(paths)} 个文件")
    
    # V3.6: 对需要智能选择的类型应用智能文件选择
    print("\n[智能文件选择] 开始筛选最佳文件...")
    smart_select_types = ["ret_login", "ret_register", "ret_fpay", "ret_play", "fpltv"]
    selected_files = {}
    
    for typ in smart_select_types:
        if file_groups[typ]:
            best_file = select_best_file_by_date_range(file_groups[typ], typ)
            if best_file:
                selected_files[typ] = [best_file]  # 只使用最佳文件
            else:
                selected_files[typ] = []
        else:
            selected_files[typ] = []
    
    # 非智能选择类型保持原有逻辑（处理所有文件）
    for typ in ["agent", "platform", "daily", "cost"]:
        selected_files[typ] = file_groups[typ]
    
    # 3) 处理选中的文件
    print("\n[处理文件]")
    agent_list, platform_list, daily_list = [], [], []
    ret_login_list, ret_register_list, ret_fpay_list, ret_play_list = [], [], [], []
    fpltv_list, cost_list = [], []
    primary_firstpay_list = []  # V3.0: 一级首充人数列表

    # 合并所有需要处理的文件
    all_selected = []
    for typ, paths in selected_files.items():
        all_selected.extend([(p, typ) for p in paths])
    
    for idx, (p, typ) in enumerate(all_selected, 1):
        print(f"  处理文件 {idx}/{len(all_selected)}: {os.path.basename(p)} ({typ})")
        df = read_any_table(p)
        result = None
        
        if typ=="agent":
            # V3.0: 传入filename参数以支持汇率换算
            result = std_agent(df, name_id_map, filename=p)
            if not result.empty:
                agent_list.append(result)
        elif typ=="platform":
            result = std_platform(df)
            if not result.empty:
                platform_list.append(result)
        elif typ=="daily":
            result = std_daily(df, name_id_map)
            if not result.empty:
                daily_list.append(result)
        elif typ=="ret_login":
            # V3.2: 传入filename参数以提取盘口
            result = std_retention(df, which="ret_login", filename=p)
            if not result.empty:
                ret_login_list.append(result)
            # V3.0: 同时提取一级首充人数（从登录留存文件）
            primary_fp = extract_primary_firstpay(df)
            if not primary_fp.empty:
                primary_firstpay_list.append(primary_fp)
        elif typ=="ret_register":
            # V3.2: 传入filename参数以提取盘口
            result = std_retention(df, which="ret_register", filename=p)
            if not result.empty:
                ret_register_list.append(result)
        elif typ=="ret_fpay":
            # V3.2: 传入filename参数以提取盘口
            result = std_retention(df, which="ret_fpay", filename=p)
            if not result.empty:
                ret_fpay_list.append(result)
        elif typ=="ret_play":  # v2.1: 新增下注留存处理
            # V3.2: 传入filename参数以提取盘口
            result = std_retention(df, which="ret_play", filename=p)
            if not result.empty:
                ret_play_list.append(result)
        elif typ=="fpltv":
            # V3.2: 传入filename参数以提取盘口
            result = std_fpltv(df, filename=p)
            if not result.empty:
                fpltv_list.append(result)
        elif typ=="cost":
            result = std_cost(df)
            if not result.empty:
                cost_list.append(result)
        else:
            # 其他未知文件忽略
            pass

    # 合并各来源（V3.5.8：修复pandas FutureWarning）
    agent = pd.concat(agent_list, ignore_index=True) if agent_list else pd.DataFrame(columns=["日期","总代名称","总代名称_清洗","总代号"])
    daily  = pd.concat(daily_list, ignore_index=True) if daily_list else pd.DataFrame(columns=["日期"])
    platform = pd.concat(platform_list, ignore_index=True) if platform_list else pd.DataFrame(columns=["日期","盘口"])
    
    # V3.5.8: 修复FutureWarning - 使用is_meaningful_df过滤
    if ret_login_list:
        non_empty = [df for df in ret_login_list if is_meaningful_df(df, required_cols=["日期","盘口","总代号"])]
        ret_login = pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame(columns=["日期"])
    else:
        ret_login = pd.DataFrame(columns=["日期"])
    
    ret_register = pd.concat(ret_register_list, ignore_index=True) if ret_register_list else pd.DataFrame(columns=["日期"])
    
    # V3.5.8: 修复FutureWarning - 使用is_meaningful_df过滤
    if ret_fpay_list:
        non_empty = [df for df in ret_fpay_list if is_meaningful_df(df, required_cols=["日期","盘口","总代号"])]
        ret_fpay = pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame(columns=["日期"])
    else:
        ret_fpay = pd.DataFrame(columns=["日期"])
    
    # V3.5.8: 修复FutureWarning - 使用is_meaningful_df过滤（ret_play）
    if ret_play_list:
        non_empty = [df for df in ret_play_list if is_meaningful_df(df, required_cols=["日期","盘口","总代号"]) ]
        ret_play = pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame(columns=["日期"])
    else:
        ret_play = pd.DataFrame(columns=["日期"])  # v2.1: 新增下注留存
    fpltv = pd.concat(fpltv_list, ignore_index=True) if fpltv_list else pd.DataFrame(columns=["日期"])
    cost  = pd.concat(cost_list, ignore_index=True) if cost_list else pd.DataFrame(columns=["日期"])
    
    # V3.5.8: 修复FutureWarning - 严格过滤primary_firstpay
    if primary_firstpay_list:
        # 更严格：过滤掉含任何全NA列的DataFrame
        def is_clean_df(df):
            if not is_meaningful_df(df, required_cols=["日期","总代号"]):
                return False
            # 额外检查：移除全NA的列后仍有数据
            cleaned = df.dropna(axis=1, how='all')
            return not cleaned.empty and cleaned.shape[1] > 0
        non_empty = [df for df in primary_firstpay_list if is_clean_df(df)]
        print(f"  [primary_firstpay过滤] {len(primary_firstpay_list)} -> {len(non_empty)} 个有效DataFrame")
        primary_firstpay = pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame(columns=["日期","总代号"])
    else:
        primary_firstpay = pd.DataFrame(columns=["日期","总代号"])
    if not primary_firstpay.empty and all(c in primary_firstpay.columns for c in ["日期", "总代号", "一级首充人数"]):
        # 按日期+总代号去重，对一级首充人数求和
        primary_firstpay = primary_firstpay.groupby(["日期", "总代号"], as_index=False)["一级首充人数"].sum()
        print(f"  Primary firstpay: {len(primary_firstpay)} rows after dedup")

    # 🔧 修复：先对agent数据去重，再构建base_keys
    # V3.0: 同时计算推广方式（从渠道名称判断）
    # V3.3: 修复关键列被错误聚合的问题
    if not agent.empty and all(c in agent.columns for c in ["日期","总代号"]):
        print(f"  Agent raw data: {len(agent)} rows")
        # 按合并键(日期+总代号)去重，对数值列求和，对文本列取第一个
        merge_keys_dedup = ["日期", "总代号"]
        text_cols = ["总代名称", "总代名称_清洗"]
        
        # V3.3: 明确保护关键文本列（盘口、推广方式等）
        protected_text_cols = []
        if "盘口" in agent.columns:
            protected_text_cols.append("盘口")
        if "推广方式" in agent.columns:
            protected_text_cols.append("推广方式")
        
        all_text_cols = text_cols + protected_text_cols
        
        # V3.0: 特殊处理渠道名称列（用于推广方式判断）
        channel_col_name = "渠道名称_原始"
        has_channel_names = channel_col_name in agent.columns
        
        # V3.3: 数值列：排除merge_keys、所有文本列、特殊列
        data_cols = [c for c in agent.columns 
                     if c not in merge_keys_dedup + all_text_cols + ([channel_col_name] if has_channel_names else [])]
        
        print(f"  [Agent去重] 文本列: {all_text_cols}")
        print(f"  [Agent去重] 数值列: {data_cols[:10]}...")  # 只显示前10个
        
        # 分别处理文本列和数值列
        agg_dict = {}
        for col in all_text_cols:
            if col in agent.columns:
                agg_dict[col] = 'first'  # 取第一个值
        
        # V3.0: 收集所有渠道名称用于推广方式判断
        if has_channel_names:
            agg_dict[channel_col_name] = lambda x: list(x)  # 收集所有渠道名称
        
        for col in data_cols:
            if col in agent.columns:
                agg_dict[col] = 'sum'  # 数值求和
        
        if agg_dict:
            agent = agent.groupby(merge_keys_dedup, as_index=False).agg(agg_dict)
            print(f"  Agent after dedup by date+id: {len(agent)} rows")
            
            # V3.3: 检查去重后的关键列
            if "盘口" in agent.columns:
                non_empty = agent["盘口"].notna().sum()
                print(f"  [Agent去重后] 盘口列: {non_empty} / {len(agent)} 非空")
                if non_empty > 0:
                    print(f"  [Agent去重后] 盘口样本: {agent['盘口'].head(3).tolist()}")
            
            # V3.0: 计算推广方式
            if has_channel_names and channel_col_name in agent.columns:
                agent["推广方式"] = agent[channel_col_name].map(get_promotion_method)
                print(f"  Added 推广方式 column based on channel names")
                # 删除临时的渠道名称列（不需要保留到最终输出）
                agent = agent.drop(columns=[channel_col_name])
    
    # 🔧 修复：对daily数据也去重
    if not daily.empty and all(c in daily.columns for c in ["日期","总代号"]):
        print(f"  Daily raw data: {len(daily)} rows")
        merge_keys_dedup = ["日期", "总代号"]
        text_cols = ["总代名称", "总代名称_清洗"]
        data_cols = [c for c in daily.columns if c not in merge_keys_dedup + text_cols]
        
        agg_dict = {}
        for col in text_cols:
            if col in daily.columns:
                agg_dict[col] = 'first'
        for col in data_cols:
            if col in daily.columns:
                agg_dict[col] = 'sum'
        
        if agg_dict:
            daily = daily.groupby(merge_keys_dedup, as_index=False).agg(agg_dict)
            print(f"  Daily after dedup by date+id: {len(daily)} rows")

    # 🔧 修复：只处理指定日期的数据（每日报表功能）
    print("\n[3.5] 筛选目标日期的数据（每日报表）...")
    all_dates = set()
    # V3.1: 添加日期分布统计
    date_stats = {}
    
    # V3.0: 添加primary_firstpay到日期筛选列表
    for name, df in [("agent", agent), ("daily", daily), ("platform", platform), 
                     ("ret_login", ret_login), ("ret_register", ret_register), 
                     ("ret_fpay", ret_fpay), ("ret_play", ret_play), 
                     ("fpltv", fpltv), ("cost", cost), ("primary_firstpay", primary_firstpay)]:
        if not df.empty and "日期" in df.columns:
            dates = df["日期"].dropna().unique()
            all_dates.update(dates)
            date_stats[name] = {str(d): len(df[df["日期"] == d]) for d in dates}
    
    # V3.1: 输出日期分布统计
    if date_stats:
        print("\n  [日期分布统计]")
        for name, dates in date_stats.items():
            if dates:
                print(f"    {name}: {dates}")
    
    if all_dates:
        # 确定要使用的日期
        if target_date and target_date != "latest":
            # 使用指定的日期
            selected_date = target_date
            if selected_date in all_dates:
                print(f"  使用指定日期: {selected_date}")
            else:
                print(f"  ⚠️ 指定日期 {selected_date} 不存在于数据中")
                print(f"  可用日期: {sorted(all_dates)}")
                print(f"  将使用最新日期代替")
                selected_date = max(all_dates)
                print(f"  实际使用日期: {selected_date}")
        else:
            # 默认日期策略（V3.5.8）：优先使用Agent的最大日期
            if not agent.empty and "日期" in agent.columns:
                selected_date = agent["日期"].dropna().max()
                print(f"  使用指定日期: {selected_date} (来源: agent)")
            else:
                selected_date = max(all_dates)
                print(f"  自动使用最新日期: {selected_date}")
        
        # 过滤所有数据源，只保留选定日期
        if not agent.empty and "日期" in agent.columns:
            before = len(agent)
            agent = agent[agent["日期"] == selected_date].copy()
            print(f"  Agent: {before} -> {len(agent)} rows")
            
            # V3.1: 如果agent数据被过滤为0，发出警告
            if len(agent) == 0 and before > 0:
                print(f"  ⚠️ 警告：Agent数据被完全过滤！")
                print(f"     - 指定日期: {selected_date}")
                print(f"     - Agent数据中的日期: {date_stats.get('agent', {})}")
                print(f"     - 建议：检查数据源文件日期是否正确，或调整TARGET_DATE配置")
        
        if not daily.empty and "日期" in daily.columns:
            before = len(daily)
            daily = daily[daily["日期"] == selected_date].copy()
            print(f"  Daily: {before} -> {len(daily)} rows")
        
        if not platform.empty and "日期" in platform.columns:
            before = len(platform)
            platform = platform[platform["日期"] == selected_date].copy()
            print(f"  Platform: {before} -> {len(platform)} rows")
        
        # V3.5.9: 留存数据保留历史记录（类似LTV处理）
        ret_login_for_base = pd.DataFrame()
        ret_login_historical = pd.DataFrame()
        if not ret_login.empty and "日期" in ret_login.columns:
            before = len(ret_login)
            ret_login["日期_dt"] = pd.to_datetime(ret_login["日期"])
            selected_dt = pd.to_datetime(selected_date)
            ret_login_for_base = ret_login[ret_login["日期_dt"] == selected_dt].copy()
            ret_login_historical = ret_login[ret_login["日期_dt"] <= selected_dt].copy()
            ret_login_for_base.drop(columns=["日期_dt"], inplace=True, errors='ignore')
            ret_login_historical.drop(columns=["日期_dt"], inplace=True, errors='ignore')
            print(f"  Retention(login)基座: {before} -> {len(ret_login_for_base)} rows (仅 {selected_date})")
            print(f"  Retention(login)历史: {before} -> {len(ret_login_historical)} rows (用于留存率倒推)")
        
        ret_register_for_base = pd.DataFrame()
        ret_register_historical = pd.DataFrame()
        if not ret_register.empty and "日期" in ret_register.columns:
            before = len(ret_register)
            ret_register["日期_dt"] = pd.to_datetime(ret_register["日期"])
            selected_dt = pd.to_datetime(selected_date)
            ret_register_for_base = ret_register[ret_register["日期_dt"] == selected_dt].copy()
            ret_register_historical = ret_register[ret_register["日期_dt"] <= selected_dt].copy()
            ret_register_for_base.drop(columns=["日期_dt"], inplace=True, errors='ignore')
            ret_register_historical.drop(columns=["日期_dt"], inplace=True, errors='ignore')
            print(f"  Retention(register)基座: {before} -> {len(ret_register_for_base)} rows (仅 {selected_date})")
            print(f"  Retention(register)历史: {before} -> {len(ret_register_historical)} rows (用于留存率倒推)")
        
        ret_fpay_for_base = pd.DataFrame()
        ret_fpay_historical = pd.DataFrame()
        if not ret_fpay.empty and "日期" in ret_fpay.columns:
            before = len(ret_fpay)
            ret_fpay["日期_dt"] = pd.to_datetime(ret_fpay["日期"])
            selected_dt = pd.to_datetime(selected_date)
            ret_fpay_for_base = ret_fpay[ret_fpay["日期_dt"] == selected_dt].copy()
            ret_fpay_historical = ret_fpay[ret_fpay["日期_dt"] <= selected_dt].copy()
            ret_fpay_for_base.drop(columns=["日期_dt"], inplace=True, errors='ignore')
            ret_fpay_historical.drop(columns=["日期_dt"], inplace=True, errors='ignore')
            print(f"  Retention(fpay)基座: {before} -> {len(ret_fpay_for_base)} rows (仅 {selected_date})")
            print(f"  Retention(fpay)历史: {before} -> {len(ret_fpay_historical)} rows (用于留存率倒推)")
        
        ret_play_for_base = pd.DataFrame()
        ret_play_historical = pd.DataFrame()
        if not ret_play.empty and "日期" in ret_play.columns:
            before = len(ret_play)
            ret_play["日期_dt"] = pd.to_datetime(ret_play["日期"])
            selected_dt = pd.to_datetime(selected_date)
            ret_play_for_base = ret_play[ret_play["日期_dt"] == selected_dt].copy()
            ret_play_historical = ret_play[ret_play["日期_dt"] <= selected_dt].copy()
            ret_play_for_base.drop(columns=["日期_dt"], inplace=True, errors='ignore')
            ret_play_historical.drop(columns=["日期_dt"], inplace=True, errors='ignore')
            print(f"  Retention(play)基座: {before} -> {len(ret_play_for_base)} rows (仅 {selected_date})")
            print(f"  Retention(play)历史: {before} -> {len(ret_play_historical)} rows (用于留存率倒推)")
        
        # V3.5.14: FPLTV分离：基座用当日数据，LTV倒推用历史数据
        fpltv_for_base = pd.DataFrame()
        fpltv_historical = pd.DataFrame()
        if not fpltv.empty and "日期" in fpltv.columns:
            before = len(fpltv)
            fpltv["日期_dt"] = pd.to_datetime(fpltv["日期"])
            selected_dt = pd.to_datetime(selected_date)
            # 只保留目标日期的数据用于基座构建
            fpltv_for_base = fpltv[fpltv["日期_dt"] == selected_dt].copy()
            # 保留完整历史数据用于LTV倒推取值
            fpltv_historical = fpltv[fpltv["日期_dt"] <= selected_dt].copy()
            fpltv_for_base.drop(columns=["日期_dt"], inplace=True, errors='ignore')
            fpltv_historical.drop(columns=["日期_dt"], inplace=True, errors='ignore')
            print(f"  FPLTV基座: {before} -> {len(fpltv_for_base)} rows (仅 {selected_date})")
            print(f"  FPLTV历史: {before} -> {len(fpltv_historical)} rows (用于LTV倒推)")
        
        if not cost.empty and "日期" in cost.columns:
            before = len(cost)
            cost = cost[cost["日期"] == selected_date].copy()
            print(f"  Cost: {before} -> {len(cost)} rows")
        
        # V3.0: 过滤一级首充人数数据
        if not primary_firstpay.empty and "日期" in primary_firstpay.columns:
            before = len(primary_firstpay)
            primary_firstpay = primary_firstpay[primary_firstpay["日期"] == selected_date].copy()
            print(f"  Primary firstpay: {before} -> {len(primary_firstpay)} rows")
        
        print(f"  ✓ 已过滤为目标日期 {selected_date} 的数据")
    else:
        print("  ⚠️ 未找到任何有效日期，将处理所有数据")

    # V3.5.5: FPLTV和留存数据去重（在日期筛选后）
    print("\n[3.6] FPLTV/留存数据去重...")
    
    # V3.5.14: 对基座用的fpltv_for_base和历史用的fpltv_historical分别去重
    if not fpltv_for_base.empty and "总代号" in fpltv_for_base.columns:
        before = len(fpltv_for_base)
        dedup_keys = ["日期"]
        if "盘口" in fpltv_for_base.columns:
            dedup_keys.append("盘口")
        if "总代号" in fpltv_for_base.columns:
            dedup_keys.append("总代号")
        
        # 按去重键分组，数值列取平均值
        numeric_cols = [c for c in fpltv_for_base.columns if c not in dedup_keys and pd.api.types.is_numeric_dtype(fpltv_for_base[c])]
        if numeric_cols:
            fpltv_for_base = fpltv_for_base.groupby(dedup_keys, as_index=False)[numeric_cols].mean()
        print(f"  FPLTV基座去重: {before} -> {len(fpltv_for_base)} 行（按{dedup_keys}）")
    
    # 对历史数据也去重（用于LTV倒推）
    if not fpltv_historical.empty and "总代号" in fpltv_historical.columns:
        before = len(fpltv_historical)
        dedup_keys = ["日期"]
        if "盘口" in fpltv_historical.columns:
            dedup_keys.append("盘口")
        if "总代号" in fpltv_historical.columns:
            dedup_keys.append("总代号")
        
        numeric_cols = [c for c in fpltv_historical.columns if c not in dedup_keys and pd.api.types.is_numeric_dtype(fpltv_historical[c])]
        if numeric_cols:
            fpltv_historical = fpltv_historical.groupby(dedup_keys, as_index=False)[numeric_cols].mean()
        print(f"  FPLTV历史去重: {before} -> {len(fpltv_historical)} 行（按{dedup_keys}）")
    
    # V3.5.9: 留存数据去重（对_for_base版本去重）
    for name, df_ret in [("ret_login", ret_login_for_base), ("ret_fpay", ret_fpay_for_base), 
                          ("ret_play", ret_play_for_base), ("ret_register", ret_register_for_base)]:
        if not df_ret.empty and "总代号" in df_ret.columns:
            before = len(df_ret)
            dedup_keys = ["日期"]
            if "盘口" in df_ret.columns:
                dedup_keys.append("盘口")
            if "总代号" in df_ret.columns:
                dedup_keys.append("总代号")
            
            # 按去重键分组，数值列取平均值
            numeric_cols = [c for c in df_ret.columns if c not in dedup_keys and pd.api.types.is_numeric_dtype(df_ret[c])]
            if numeric_cols:
                df_ret = df_ret.groupby(dedup_keys, as_index=False)[numeric_cols].mean()
                
                # 更新原DataFrame（更新_for_base版本）
                if name == "ret_login":
                    ret_login_for_base = df_ret
                elif name == "ret_fpay":
                    ret_fpay_for_base = df_ret
                elif name == "ret_play":
                    ret_play_for_base = df_ret
                elif name == "ret_register":
                    ret_register_for_base = df_ret
                
                print(f"  {name}基座去重: {before} -> {len(df_ret)} 行（按{dedup_keys}）")

    # 3) 生成主键并广播平台级
    # V3.5.12: 以agent为主基座，从其他来源补充缺失组合
    print("\n[V3.5.12] 构建基座：主基座(agent) + 补充(ret/fpltv)...")
    
    # 1) agent作为主基座（保留完整信息）
    if not agent.empty:
        base_cols = ["日期","总代名称","总代名称_清洗","总代号"]
        if "盘口" in agent.columns:
            base_cols.append("盘口")
        if "推广部门" in agent.columns:  # V3.6: 保留推广部门
            base_cols.append("推广部门")
        if "推广方式" in agent.columns:
            base_cols.append("推广方式")
        
        base_keys = agent[base_cols].drop_duplicates()
        agent_platforms = base_keys["盘口"].nunique() if "盘口" in base_keys.columns else 0
        agent_agents = base_keys["总代号"].nunique() if "总代号" in base_keys.columns else 0
        print(f"  主基座(agent): {len(base_keys)}行 / {agent_platforms}盘口 / {agent_agents}总代")
    else:
        base_keys = pd.DataFrame(columns=["日期","总代名称","总代名称_清洗","总代号","盘口","推广部门","推广方式"])
        print(f"  警告：agent为空，基座从其他来源构建")
    
    # 2) 从其他来源补充"在它们有但agent无"的组合
    # 预先准备agent的总代名称映射表（跨日期/盘口，按总代号查找名称）
    if not agent.empty and "总代号" in agent.columns:
        agent_name_map = agent[["总代号","总代名称","总代名称_清洗"]].dropna(subset=["总代号"]).drop_duplicates("总代号")
    else:
        agent_name_map = pd.DataFrame(columns=["总代号","总代名称","总代名称_清洗"])
    
    # V3.5.14: 基座补充使用fpltv_for_base和ret_*_for_base（只含目标日期）
    supplement_sources = [(ret_login_for_base, 'ret_login'), (ret_play_for_base, 'ret_play'), 
                          (ret_fpay_for_base, 'ret_fpay'), (fpltv_for_base, 'fpltv')]
    
    total_supplemented = 0
    for src, name in supplement_sources:
        if not isinstance(src, pd.DataFrame) or src.empty:
            continue
        if "盘口" not in src.columns or "总代号" not in src.columns or "日期" not in src.columns:
            continue
        
        # 该来源的[日期,盘口,总代号]组合
        src_keys = src[["日期","盘口","总代号"]].drop_duplicates()
        
        # anti-join：找出"在src但不在base_keys"的组合
        if not base_keys.empty and "盘口" in base_keys.columns and "总代号" in base_keys.columns:
            merged = src_keys.merge(
                base_keys[["日期","盘口","总代号"]], 
                on=["日期","盘口","总代号"], 
                how="left", 
                indicator=True
            )
            new_keys = merged[merged["_merge"] == "left_only"][["日期","盘口","总代号"]].copy()
        else:
            new_keys = src_keys.copy()
        
        if not new_keys.empty:
            # 从agent回填总代名称（按总代号匹配）
            if not agent_name_map.empty:
                new_keys = new_keys.merge(agent_name_map, on="总代号", how="left")
            else:
                new_keys["总代名称"] = None
                new_keys["总代名称_清洗"] = None
            
            new_keys["推广方式"] = None  # 后续从渠道名称推断
            
            # 添加到基座
            base_keys = pd.concat([base_keys, new_keys], ignore_index=True)
            total_supplemented += len(new_keys)
            print(f"  补充({name}): +{len(new_keys)}行")
    
    if total_supplemented > 0:
        final_platforms = base_keys["盘口"].nunique() if "盘口" in base_keys.columns else 0
        final_agents = base_keys["总代号"].nunique() if "总代号" in base_keys.columns else 0
        print(f"\n  ✓ 基座最终: {len(base_keys)}行 / {final_platforms}盘口 / {final_agents}总代（补充+{total_supplemented}）")
    
    # Check if base_keys is empty
    print(f"  Base keys shape: {base_keys.shape if isinstance(base_keys, pd.DataFrame) else 'None'}")
    if base_keys.empty:
        print("  Error: No valid data found. Cannot generate report.")
        return
    
    # 确保必要列存在
    for col in ["总代名称_清洗", "总代名称", "总代号", "推广方式", "盘口"]:
        if col not in base_keys.columns:
            base_keys[col] = None
    
    print(f"  Base keys: {len(base_keys)} unique combinations")
    
    # 解析命名串（产品/部门/方式等）
    parse_df = base_keys.copy()
    # V3.5.11: 只对非空的总代名称进行解析
    # V3.6: 如果推广部门已存在（从平台映射），保留；否则从总代名称解析（兜底）
    def safe_parse(name):
        if pd.isna(name) or name is None:
            return pd.Series({"产品": None, "推广部门_parsed": None})
        result = parse_channel_clean(name)
        return pd.Series({"产品": result["产品"], "推广部门_parsed": result["推广部门"]})
    
    parsed = parse_df["总代名称_清洗"].apply(safe_parse)
    parse_df = pd.concat([parse_df, parsed], axis=1)
    
    # V3.6: 推广部门优先级：1) 从agent/平台映射的 2) 从总代名称解析的（兜底）
    if "推广部门" not in parse_df.columns:
        parse_df["推广部门"] = parse_df["推广部门_parsed"]
    else:
        # 有推广部门但为空的行，用解析结果填充
        parse_df["推广部门"] = parse_df["推广部门"].fillna(parse_df["推广部门_parsed"])
    
    # 删除临时列
    if "推广部门_parsed" in parse_df.columns:
        parse_df = parse_df.drop(columns=["推广部门_parsed"])
    
    print(f"  After parsing: {len(parse_df)} rows, 推广部门非空: {parse_df['推广部门'].notna().sum()}")
    
    # V3.5.12: 确保产品字段填充"TT产品"
    if "产品" in parse_df.columns:
        before_fill = parse_df["产品"].notna().sum()
        parse_df["产品"] = parse_df["产品"].fillna("TT产品")
        after_fill = parse_df["产品"].notna().sum()
        print(f"  产品字段填充: {before_fill} -> {after_fill} 非空（默认'TT产品'）")
    else:
        parse_df["产品"] = "TT产品"
        print(f"  产品字段创建: 统一为'TT产品'")

    # V3.2: 盘口字段优先级调整
    # V3.3: 简化逻辑，修复列冲突问题
    # V3.4: 如果base_keys已包含盘口，直接使用
    
    print("\n[盘口赋值]")
    
    # 准备一个平台来源按[日期,盘口]的唯一键，方便后续广播
    plat_keys = platform[["日期","盘口"]].dropna().drop_duplicates() if not platform.empty else pd.DataFrame(columns=["日期","盘口"])
    
    # V3.4: 优先级1 - 如果base_keys已经包含盘口（来自agent，从文件名提取），直接使用
    if "盘口" in parse_df.columns:
        non_empty = parse_df["盘口"].notna().sum()
        print(f"  ✓ Base_keys中已有盘口: {non_empty} / {len(parse_df)} 非空")
        if non_empty > 0:
            print(f"  盘口样本: {parse_df['盘口'].dropna().head(5).tolist()}")
    else:
        # 优先级2: 从parse_channel_clean获取
        if "盘口_token" in parse_df.columns:
            parse_df["盘口"] = parse_df["盘口_token"]
            print(f"  从渠道名称解析: {parse_df['盘口'].notna().sum()} 条")
        else:
            parse_df["盘口"] = None
        
        # 优先级3: 用"部门→盘口"映射（最后兜底）
        if "推广部门" in parse_df.columns and parse_df["盘口"].isna().any():
            parse_df["盘口"] = parse_df["盘口"].fillna(parse_df["推广部门"].map(DEPT_TO_PLATFORM))
            print(f"  从部门映射补充: {parse_df['盘口'].notna().sum()} 条")

    # 4) 组建主表并左连接各指标
    main = parse_df.copy()  # 含：日期、总代名称(_清洗)、总代号、产品、推广部门、推广方式、盘口(可能为空)
    print(f"  Main table initialized: {len(main)} rows, {len(main.columns)} columns")
    
    # V3.3: 数据验证检查点
    print("\n[数据验证 - Main表初始化后]")
    valid_ids = main["总代号"].notna().sum()
    print(f"  总代号非空: {valid_ids} / {len(main)}")
    if "盘口" in main.columns:
        valid_platforms = main["盘口"].notna().sum()
        print(f"  盘口非空: {valid_platforms} / {len(main)}")
        if valid_platforms > 0:
            unique_platforms = main["盘口"].dropna().unique().tolist()
            print(f"  盘口唯一值({len(unique_platforms)}个): {unique_platforms[:10]}")
    if "推广部门" in main.columns:
        valid_depts = main["推广部门"].notna().sum()
        print(f"  推广部门非空: {valid_depts} / {len(main)}")
    
    # 若缺ID，用稳定ID兜底，后续主键与聚合才可用
    if valid_ids < len(main):
        main["总代号"] = main["总代号"].where(
            main["总代号"].notna(),
            main["总代名称_清洗"].map(stable_agent_id)
        )
        print(f"  After stable ID fill: {main['总代号'].notna().sum()} / {len(main)} have IDs")
    
    # Determine merge strategy: if no valid IDs, use name_clean instead
    use_id_merge = main["总代号"].notna().sum() > 0
    merge_keys = ["日期", "总代号"] if use_id_merge else ["日期", "总代名称_清洗"]
    print(f"  Using merge keys: {'date+id' if use_id_merge else 'date+name_clean'}")
    
    # 代理：注册/活跃/充值/首充/提现
    if not agent.empty:
        print(f"\n[Agent数据合并]")
        print(f"  Agent数据行数: {len(agent)}")
        agent_merge_cols = [c for c in merge_keys if c in agent.columns]
        if agent_merge_cols and all(k in agent.columns for k in merge_keys):
            # 只选择存在的列
            data_cols = [c for c in ["注册人数","活跃人数","充值人数","充值金额","首充人数","当日首充金额","提现金额","充提差"] if c in agent.columns]
            print(f"  可用数据列: {data_cols}")
            
            if data_cols:
                tmp = agent[merge_keys + data_cols].copy()
                # 注意：agent数据已经在前面按date+id去重了，这里不需要再去重
                
                # V3.3: 详细调试信息
                print(f"  [合并前] Agent数据: {len(tmp)} 行")
                if '充值金额' in tmp.columns:
                    non_zero = (tmp['充值金额'] > 0).sum()
                    print(f"  [合并前] 充值金额非零行: {non_zero}, 总和: {tmp['充值金额'].sum():.2f}")
                if '注册人数' in tmp.columns:
                    non_zero = (tmp['注册人数'] > 0).sum()
                    print(f"  [合并前] 注册人数非零行: {non_zero}, 总和: {tmp['注册人数'].sum()}")
                
                # 检查合并键匹配
                main_keys_set = set(zip(main[merge_keys[0]], main[merge_keys[1]]))
                tmp_keys_set = set(zip(tmp[merge_keys[0]], tmp[merge_keys[1]]))
                matched_keys = main_keys_set & tmp_keys_set
                print(f"  [合并键匹配] Main={len(main_keys_set)}, Agent={len(tmp_keys_set)}, 匹配={len(matched_keys)}")
                
                if len(matched_keys) == 0:
                    print(f"  ❌ 警告：合并键完全不匹配！")
                    print(f"  Main样本键: {list(main_keys_set)[:3]}")
                    print(f"  Agent样本键: {list(tmp_keys_set)[:3]}")
                
                # 执行merge
                before_rows = len(main)
                main = main.merge(tmp, on=merge_keys, how="left")
                print(f"  [合并后] Main数据: {before_rows} -> {len(main)} 行")
                
                # 合并后的数据
                if '充值金额' in main.columns:
                    non_zero = (main['充值金额'] > 0).sum()
                    print(f"  [合并后] 充值金额非零行: {non_zero}, 总和: {main['充值金额'].sum():.2f}")
                if '注册人数' in main.columns:
                    non_zero = (main['注册人数'] > 0).sum()
                    print(f"  [合并后] 注册人数非零行: {non_zero}, 总和: {main['注册人数'].sum()}")
            else:
                print(f"  ⚠️ 跳过：没有可用的数据列")
        else:
            print(f"  ⚠️ 跳过：缺少合并键 {merge_keys}")

    # 日常：首充、活跃充值
    if not daily.empty:
        daily_keys = [k for k in merge_keys if k in daily.columns]
        if daily_keys and len(daily_keys) == len(merge_keys):
            cols = daily_keys + [c for c in ["当日首充金额","首充人数","活跃充值人数"] if c in daily.columns]
            tmp = daily[cols].copy()
            # 注意：daily数据已经在前面按date+id去重了，这里不需要再去重
            
            main = main.merge(tmp, on=daily_keys, how="left", suffixes=("",""))
    
    # V3.0: 一级首充人数（V3.5.9: 添加详细诊断）
    if not primary_firstpay.empty:
        print(f"\n[一级首充合并]")
        print(f"  primary_firstpay数据行数: {len(primary_firstpay)}")
        primary_keys = [k for k in merge_keys if k in primary_firstpay.columns]
        if primary_keys and len(primary_keys) == len(merge_keys):
            cols = primary_keys + ["一级首充人数"]
            tmp = primary_firstpay[cols].copy()
            
            # 诊断：检查合并键匹配
            main_keys_set = set(zip(*[main[k] for k in primary_keys]))
            tmp_keys_set = set(zip(*[tmp[k] for k in primary_keys]))
            matched_keys = main_keys_set & tmp_keys_set
            print(f"  合并键: {primary_keys}")
            print(f"  Main键数量: {len(main_keys_set)}, primary_firstpay键数量: {len(tmp_keys_set)}, 匹配数: {len(matched_keys)}")
            
            before_rows = len(main)
            main = main.merge(tmp, on=primary_keys, how="left")
            print(f"  合并后: {before_rows} -> {len(main)} rows")
            
            # 检查一级首充人数列
            if "一级首充人数" in main.columns:
                non_zero = (main["一级首充人数"] > 0).sum()
                total = main["一级首充人数"].sum()
                print(f"  一级首充人数: {non_zero}/{len(main)} 非零, 总计: {total}")
            else:
                print(f"  ⚠️ 警告：一级首充人数列未成功合并")
        else:
            print(f"  ⚠️ 跳过：合并键不匹配 {primary_keys}")

    # 成本/广告（既支持渠道级，也支持平台级）
    # 渠道级
    if not cost.empty and "总代名称_清洗" in cost.columns:
        tmp = cost[["日期","总代名称_清洗","消耗","展示","点击","提现金额"]].copy()
        
        # 🔧 修复：合并前去重
        data_cols = ["消耗","展示","点击","提现金额"]
        data_cols = [c for c in data_cols if c in tmp.columns]
        if data_cols:
            tmp = tmp.groupby(["日期","总代名称_清洗"], as_index=False)[data_cols].sum()
            print(f"  Cost (channel) data after dedup: {len(tmp)} rows")
        
        main = main.merge(tmp, on=["日期","总代名称_清洗"], how="left")
    # 平台级（广播）
    if not cost.empty and "盘口" in cost.columns:
        tmp = cost[["日期","盘口","消耗","展示","点击","提现金额"]].copy()
        
        # 🔧 修复：合并前去重
        data_cols = ["消耗","展示","点击","提现金额"]
        data_cols = [c for c in data_cols if c in tmp.columns]
        if data_cols:
            tmp = tmp.groupby(["日期","盘口"], as_index=False)[data_cols].sum()
            print(f"  Cost (platform) data after dedup: {len(tmp)} rows")
        
        # 防止覆盖渠道级的非空数值：只在空值处用平台级补
        main = main.merge(tmp, on=["日期","盘口"], how="left", suffixes=("","_plat"))
        for col in ["消耗","展示","点击","提现金额"]:
            if col+"_plat" in main.columns:
                main[col] = main[col].fillna(main[col+"_plat"])
                main.drop(columns=[col+"_plat"], inplace=True)

    # 平台LTV（广播）
    if not platform.empty:
        tmp = platform.copy()
        # 映射到标准列名
        rename_map = {"ltv_D1":"首充当日ltv替代_D1",
                      "ltv_D3":"平台LTV_D3","ltv_D7":"平台LTV_D7","ltv_D14":"平台LTV_D14","ltv_D30":"平台LTV_D30"}
        tmp.rename(columns=rename_map, inplace=True)
        
        # 🔧 修复：合并前去重
        merge_cols = ["日期","盘口"]
        data_cols = [c for c in list(rename_map.values()) if c in tmp.columns]
        if data_cols:
            tmp = tmp[merge_cols + data_cols].groupby(merge_cols, as_index=False)[data_cols].mean()
            print(f"  Platform LTV data after dedup: {len(tmp)} rows")
        
        main = main.merge(tmp, on=["日期","盘口"], how="left")

    # V3.5.14: 首充LTV倒推（使用fpltv_historical历史数据）
    if not fpltv_historical.empty:
        if "日期" in fpltv_historical.columns and 'selected_date' in locals():
            print(f"\n[FPLTV倒推] 按偏移日精确取值（不改日期，仅取值）...")
            fpltv_historical["日期_dt"] = pd.to_datetime(fpltv_historical["日期"])
            selected_dt = pd.to_datetime(selected_date)

            # 列识别诊断
            ltv_cols_all = [c for c in ["FPLTV_D1","FPLTV_D2","FPLTV_D3","FPLTV_D7","FPLTV_D14","FPLTV_D15","FPLTV_D30"] if c in fpltv_historical.columns]
            print(f"  [FPLTV列识别] 已识别列: {ltv_cols_all}")

            # 需要的偏移天数与列映射
            need_map = {
                1: "FPLTV_D1",
                2: "FPLTV_D2",
                3: "FPLTV_D3",
                7: "FPLTV_D7",
                14: "FPLTV_D14",
                15: "FPLTV_D15",
                30: "FPLTV_D30",
            }

            # 合并键（不使用日期）
            merge_keys = []
            if "总代号" in fpltv_historical.columns and "总代号" in main.columns and "盘口" in fpltv_historical.columns and "盘口" in main.columns:
                merge_keys = ["盘口","总代号"]
            elif "盘口" in fpltv_historical.columns and "盘口" in main.columns:
                merge_keys = ["盘口"]
            else:
                print("  [FPLTV] 缺少合并键（盘口/总代号），跳过")
                merge_keys = []

            # 针对每个偏移日，精确取目标日期 = selected_date - n 的一行，再贴数值
            if merge_keys:
                for offset, col in need_map.items():
                    if col not in fpltv_historical.columns:
                        print(f"  [FPLTV] 缺少列 {col}，跳过该偏移{offset}日")
                        continue
                    cutoff_dt = selected_dt - pd.Timedelta(days=offset)
                    sub = fpltv_historical[fpltv_historical["日期_dt"] <= cutoff_dt]
                    if sub.empty:
                        print(f"  [FPLTV] 偏移{offset}日在 ≤ {cutoff_dt.date()} 无可用行")
                        continue
                    # 取每个键在截止日前的最新一行
                    sub = sub.sort_values("日期_dt").groupby(merge_keys, as_index=False).last()
                    # 只保留需要的列
                    keep_cols = [c for c in (merge_keys + [col]) if c in sub.columns]
                    sub = sub[keep_cols].copy()
                    print(f"  [FPLTV] 偏移{offset}日可用: {len(sub)} 行（截止 {cutoff_dt.date()}）")
                    main = main.merge(sub, on=merge_keys, how="left")

                # 诊断：贴完后统计非零
                for col in ["FPLTV_D7","FPLTV_D14","FPLTV_D15","FPLTV_D30"]:
                    if col in main.columns:
                        non_zero = (pd.to_numeric(main[col], errors='coerce').fillna(0) != 0).sum()
                        print(f"  [FPLTV] 合并后 {col}: 非零 {non_zero}/{len(main)}")
        else:
            print(f"  [警告] FPLTV缺少日期列或未设置selected_date，跳过倒推")

    # V3.5.9: 留存（用于复登率_偏移：使用_historical版本进行倒推取值）
    # 优先用首充留存；缺则用下注留存；再缺用首登留存；最后用注册留存
    # v2.1: 新增ret_play选项
    ret_pick = {}
    if not ret_fpay_historical.empty:
        ret_pick = ret_fpay_historical.copy()
    elif not ret_play_historical.empty:  # v2.1: 新增优先级
        ret_pick = ret_play_historical.copy()
    elif not ret_login_historical.empty:
        ret_pick = ret_login_historical.copy()
    elif not ret_register_historical.empty:
        ret_pick = ret_register_historical.copy()
    if isinstance(ret_pick, pd.DataFrame) and not ret_pick.empty:
        # V3.5.11: 不再使用日期作为合并键，只用[盘口, 总代号]或[盘口]
        merge_keys = []
        if "总代号" in ret_pick.columns and "总代号" in main.columns:
            # 优先使用总代号精确匹配
            if "盘口" in ret_pick.columns and "盘口" in main.columns:
                merge_keys = ["盘口", "总代号"]
            else:
                merge_keys = ["总代号"]
            print(f"  [留存] 使用[盘口+总代号]维度合并")
        elif "盘口" in ret_pick.columns and "盘口" in main.columns:
            # 降级：按盘口广播
            merge_keys = ["盘口"]
            print(f"  [留存] 降级为[盘口]维度合并")
        
        keep = merge_keys + [c for c in ret_pick.columns if any(k in c for k in ["D1","D3","D7","D15","D30"])]
        keep = [c for c in keep if c in ret_pick.columns]  # Filter to existing columns
        tmp = ret_pick[keep].copy()
        
        # 🔧 修复：合并前去重，避免重复文件导致笛卡尔积
        # 按合并键去重，对数值列取平均值
        if len(merge_keys) >= 1:
            numeric_cols = [c for c in tmp.columns if c not in merge_keys]
            if numeric_cols:
                # 聚合：对重复的合并键，数值列取平均
                tmp = tmp.groupby(merge_keys, as_index=False)[numeric_cols].mean()
            else:
                # 如果没有数值列，直接去重
                tmp = tmp.drop_duplicates(subset=merge_keys, keep='first')
            
            print(f"  [留存] 去重后数据: {len(tmp)} 行")
            print(f"  [留存] 合并键: {merge_keys}, 数据行数: {len(tmp)}")
            main = main.merge(tmp, on=merge_keys, how="left", suffixes=("","_ret"))
        else:
            print(f"  [警告] 留存数据缺少有效合并键，跳过合并")

    # 5) 指标计算与缺省处理
    def num(col_name, default_val=0): 
        if col_name in main.columns:
            return pd.to_numeric(main[col_name], errors="coerce").fillna(default_val)
        else:
            return pd.Series([default_val] * len(main), index=main.index)

    # 衍生
    main["注册人数"]     = num("注册人数", 0).astype("Int64")
    main["活跃人数"]     = num("活跃人数", 0).astype("Int64")
    main["充值人数"]     = num("充值人数", 0).astype("Int64")
    main["充值金额"]     = num("充值金额", 0.0)
    main["当日首充金额"] = num("当日首充金额", 0.0)
    main["首充人数"]     = num("首充人数", 0).astype("Int64")
    main["展示"]         = num("展示", 0).astype("Int64")
    main["点击"]         = num("点击", 0).astype("Int64")
    main["消耗"]         = num("消耗", 0.0)
    main["提现金额"]     = num("提现金额", 0.0)

    # —— 充提差：如果提现金额有来源：充值金额 - 提现金额；否则按 0（或使用你们的其他公式）
    main["充提差"] = (main["充值金额"] - main["提现金额"]).fillna(0.0)

    # 千展成本crm
    main["千展成本crm"] = main.apply(lambda r: (r["消耗"] / (r["展示"]/1000.0)) if r["展示"]>0 else 0.0, axis=1)
    # 点击率
    main["点击率"] = main.apply(lambda r: (r["点击"] / r["展示"]) if r["展示"]>0 else 0.0, axis=1)
    # 注册成本 / 首充成本
    main["注册成本"]   = main.apply(lambda r: (r["消耗"] / r["注册人数"]) if r["注册人数"] and r["注册人数"]>0 else 0.0, axis=1)
    main["首充成本"]   = main.apply(lambda r: (r["消耗"] / r["首充人数"]) if r["首充人数"] and r["首充人数"]>0 else 0.0, axis=1)
    # V3.5.9: 一级首充人数/成本（从primary_firstpay合并，已在前面处理）
    # 如果合并失败，兜底为0
    if "一级首充人数" not in main.columns:
        main["一级首充人数"] = 0
    main["一级首充人数"] = num("一级首充人数", 0).astype("Int64")
    main["一级首充成本"] = main.apply(lambda r: (r["消耗"] / r["一级首充人数"]) if r["一级首充人数"] and r["一级首充人数"]>0 else 0.0, axis=1)

    # 首充转化率 / 首充arppu / 首充roas
    main["首充转化率"] = main.apply(lambda r: (r["首充人数"]/r["注册人数"]) if r["注册人数"] and r["注册人数"]>0 else 0.0, axis=1)
    main["首充arppu"] = main.apply(lambda r: (r["当日首充金额"]/r["首充人数"]) if r["首充人数"] and r["首充人数"]>0 else 0.0, axis=1)
    main["首充roas"] = main.apply(lambda r: (r["当日首充金额"]/r["消耗"]) if r["消耗"]>0 else 0.0, axis=1)

    # 首充当日ltv（优先用首充LTV_D1；没有就用平台LTV_D1 替代）
    main["首充当日ltv"] = num("FPLTV_D1", 0.0)

    # 首充当日roi（按你的口径可改；这里示例用：首充当日充提差 / 消耗）
    # 先算 首充当日充提差 = 当日首充金额 - 当日首提金额
    # V3.5.10: 支持 scale 模式估算首充用户提现
    if WITHDRAW_APPROX_MODE == "scale":
        # 按比例估算：首充用户提现 ≈ 总提现 × (首充人数/充值人数)
        withdraw_ratio = main.apply(lambda r: r["首充人数"]/r["充值人数"] if r["充值人数"]>0 else 0, axis=1)
        estimated_fpay_withdraw = main["提现金额"] * withdraw_ratio
        main["首充当日充提差"] = (main["当日首充金额"] - estimated_fpay_withdraw).fillna(0.0)
        print(f"  [首充充提差] 使用scale模式估算（首充人数/充值人数比例）")
    else:
        # zero模式：不估算提现，首充充提差=首充金额
        main["首充当日充提差"] = main["当日首充金额"]
        print(f"  [首充充提差] 使用zero模式（不估算提现）")
    
    main["首充当日roi"] = main.apply(lambda r: (r["首充当日充提差"]/r["消耗"]) if r["消耗"]>0 else 0.0, axis=1)

    # 首充充提差比 = 首充当日充提差 / 当日首充金额
    main["首充充提差比"] = main.apply(lambda r: (r["首充当日充提差"]/r["当日首充金额"]) if r["当日首充金额"]>0 else 0.0, axis=1)

    # 偏移 LTV（来自 FPLTV）
    main["首充两日ltv_偏移"]   = num("FPLTV_D2", 0.0)
    main["首充三日ltv_偏移"]   = num("FPLTV_D3", 0.0)
    main["首充七日ltv_偏移"]   = num("FPLTV_D7", 0.0)
    # 15日：优先用D15，缺则用D14
    if "FPLTV_D15" in main.columns:
        main["首充十五日ltv_偏移"] = num("FPLTV_D15", 0.0)
    else:
        main["首充十五日ltv_偏移"] = 0.0
    if ("FPLTV_D14" in main.columns):
        main["首充十五日ltv_偏移"] = main["首充十五日ltv_偏移"].where(
            pd.to_numeric(main["首充十五日ltv_偏移"], errors='coerce').fillna(0) != 0,
            num("FPLTV_D14", 0.0)
        )
    main["首充三十日ltv_偏移"] = num("FPLTV_D30", 0.0)

    # 偏移 复登/复投/复充率：严格分来源；支持两种口径（retention/formula）
    def _get_ret(prefix: str, day: int):
        col = f"{prefix}_D{day}"
        return num(col, 0.0) if col in main.columns else pd.Series(0.0, index=main.index)

    reg = pd.to_numeric(main["注册人数"], errors="coerce")
    fpu = pd.to_numeric(main["首充人数"], errors="coerce")
    ratio = (fpu / reg.replace(0, pd.NA)).fillna(0.0)

    def _apply_offset(series: pd.Series) -> pd.Series:
        if OFFSET_MODE == "formula":
            return (series * ratio).fillna(0.0)
        return series.fillna(0.0)

    # 复登率_偏移 ← ret_login
    main["首充次日复登率_偏移"]   = _apply_offset(_get_ret("ret_login", 1))
    main["首充三日复登率_偏移"]   = _apply_offset(_get_ret("ret_login", 3))
    main["首充七日复登率_偏移"]   = _apply_offset(_get_ret("ret_login", 7))
    main["首充十五日复登率_偏移"] = _apply_offset(_get_ret("ret_login", 15))
    main["首充三十日复登率_偏移"] = _apply_offset(_get_ret("ret_login", 30))

    # 复投率_偏移 ← ret_play
    main["首充次日复投率_偏移"]   = _apply_offset(_get_ret("ret_play", 1))
    main["首充三日复投率_偏移"]   = _apply_offset(_get_ret("ret_play", 3))
    main["首充七日复投率_偏移"]   = _apply_offset(_get_ret("ret_play", 7))
    main["首充十五日复投率_偏移"] = _apply_offset(_get_ret("ret_play", 15))
    main["首充三十日复投率_偏移"] = _apply_offset(_get_ret("ret_play", 30))

    # 复充率_偏移 ← ret_fpay
    main["首充次日复充率_偏移"]   = _apply_offset(_get_ret("ret_fpay", 1))
    main["首充三日复充率_偏移"]   = _apply_offset(_get_ret("ret_fpay", 3))
    main["首充七日复充率_偏移"]   = _apply_offset(_get_ret("ret_fpay", 7))
    main["首充十五日复充率_偏移"] = _apply_offset(_get_ret("ret_fpay", 15))
    main["首充三十日复充率_偏移"] = _apply_offset(_get_ret("ret_fpay", 30))

    # 累计roas / 自然月消耗（按窗口累计）
    # V3.5.10: 实现真实累计ROAS计算
    print(f"  [累计ROAS] 计算按总代号+盘口的累计充值和消耗...")
    main = main.sort_values(["总代号", "盘口", "日期"])
    main["累计充值金额"] = main.groupby(["总代号", "盘口"])["充值金额"].cumsum()
    main["累计消耗"] = main.groupby(["总代号", "盘口"])["消耗"].cumsum()
    main["累计roas"] = main.apply(
        lambda r: r["累计充值金额"]/r["累计消耗"] if r["累计消耗"]>0 else 0.0, axis=1)
    
    # 自然月消耗
    main["自然月"] = main["日期"].str.slice(0, 7)
    main["自然月消耗"] = main.groupby(["总代号", "盘口", "自然月"])["消耗"].transform("sum")

    # V3.5.9: 非一级首充占比（计算公式）
    main["非一级首充人数"] = (main["首充人数"] - main["一级首充人数"]).clip(lower=0).astype("Int64")
    main["非一级首充人数/首充人数"] = main.apply(
        lambda r: (r["非一级首充人数"]/r["首充人数"]) if r["首充人数"] and r["首充人数"]>0 else 0.0, axis=1)
    main["非一级首充人数/充值人数"] = main.apply(
        lambda r: (r["非一级首充人数"]/r["充值人数"]) if r["充值人数"] and r["充值人数"]>0 else 0.0, axis=1)

    # 6) 填充其他维度列
    main["产品"] = main["产品"].fillna("")
    main["推广方式"] = main["推广方式"].fillna("")
    main["总代名称"] = main["总代名称"].fillna(main["总代名称_清洗"])
    # 日期必须存在，总代号或总代名称至少有一个
    main = main.dropna(subset=["日期"])
    # If no valid IDs, at least need name_clean
    if valid_ids == 0:
        main = main.dropna(subset=["总代名称_清洗"])

    # 7) 输出 53 列（缺失列补 0/空），顺序锁定
    print(f"  Before adding missing columns: {len(main)} rows")
    for col in FINAL_COLUMNS:
        if col not in main.columns:
            # 默认缺失：数值 0，文本空
            if col in ["日期","产品","盘口","总代名称","推广部门","推广方式"]:
                main[col] = ""
            else:
                main[col] = 0.0
    print(f"  After adding missing columns: {len(main)} rows")

    # 列类型微调：整数列
    for icol in ["注册人数","首充人数","一级首充人数","充值人数","展示","点击"]:
        if icol in main.columns:
            main[icol] = pd.to_numeric(main[icol], errors="coerce").fillna(0).astype("Int64")

    # 聚合与去重：统一按 [日期, 盘口, 总代号] 进行分组聚合
    print(f"  Before aggregation: {len(main)} rows")
    
    # 调试：聚合前的数据统计
    if '充值金额' in main.columns:
        print(f"  Before agg - 充值金额总和: {main['充值金额'].sum():.2f}")
    
    if len(main) > 0:
        group_cols = [c for c in ["日期", "盘口", "总代号"] if c in main.columns]
        if group_cols:
            # V3.5.8: 简化且健壮的标量提取逻辑
            def flatten_to_scalar(value):
                """递归展开DataFrame/Series/容器类型至标量，最多10层防止无限循环"""
                import numpy as np
                for _ in range(10):  # 最多10次递归
                    # DataFrame → 取[0,0]
                    if isinstance(value, pd.DataFrame):
                        if value.shape[0] > 0 and value.shape[1] > 0:
                            value = value.iloc[0, 0]
                            continue
                        return None
                    
                    # Series → 取第一个非空（或第一个）
                    if isinstance(value, pd.Series):
                        if len(value) == 0:
                            return None
                        non_null = value.dropna()
                        value = non_null.iloc[0] if len(non_null) > 0 else value.iloc[0]
                        continue
                    
                    # 容器类型（list/tuple/ndarray） → 取第一个非空元素
                    if isinstance(value, (list, tuple, np.ndarray)):
                        if len(value) == 0:
                            return None
                        seq = list(value)
                        for item in seq:
                            if item is not None and not (isinstance(item, float) and pd.isna(item)):
                                value = item
                                break
                        else:
                            value = seq[0]
                        continue
                    
                    # dict → 取第一个value
                    if isinstance(value, dict):
                        try:
                            value = next(iter(value.values()))
                            continue
                        except StopIteration:
                            return None
                    
                    # 已是标量，跳出循环
                    break
                
                # 最终检查与返回
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return None
                
                # 确保是Python标量类型
                import numpy as np
                if isinstance(value, (str, bytes, int, float, bool)):
                    return value
                if hasattr(np, 'isscalar') and np.isscalar(value):
                    return value
                
                # 最后兜底：强制转字符串
                return str(value)

            def safe_first(series):
                """安全地取第一个非空值（确保返回标量，V3.5.8强化版 + 最终兜底）"""
                if isinstance(series, pd.DataFrame):
                    if len(series.columns) > 0:
                        series = series.iloc[:, 0]
                    else:
                        return None
                non_null = series.dropna()
                if len(non_null) > 0:
                    result = flatten_to_scalar(non_null.iat[0])
                    # 最终兜底：若仍是DataFrame/Series，强制提取标量
                    if isinstance(result, pd.DataFrame):
                        try:
                            return str(result.iloc[0, 0]) if result.shape[0] > 0 and result.shape[1] > 0 else None
                        except:
                            return str(result)
                    elif isinstance(result, pd.Series):
                        try:
                            return str(result.iloc[0]) if len(result) > 0 else None
                        except:
                            return str(result)
                    return result
                if len(series) > 0:
                    result = flatten_to_scalar(series.iat[0])
                    # 同样的兜底逻辑
                    if isinstance(result, pd.DataFrame):
                        try:
                            return str(result.iloc[0, 0]) if result.shape[0] > 0 and result.shape[1] > 0 else None
                        except:
                            return str(result)
                    elif isinstance(result, pd.Series):
                        try:
                            return str(result.iloc[0]) if len(result) > 0 else None
                        except:
                            return str(result)
                    return result
                return None
            
            # 构建聚合规则
            agg_dict = {}
            for col in main.columns:
                if col in group_cols:
                    continue
                    
                if pd.api.types.is_numeric_dtype(main[col]):
                    # 数值列：求和
                    agg_dict[col] = 'sum'
                else:
                    # 文本列：使用自定义函数（避免'first'的bug）
                    agg_dict[col] = safe_first
            
            # V3.5.8: 聚合前防御 - 合并重复列 + 深度清理非标量
            print(f"  [聚合前诊断] 合并重复列并清理非标量值...")
            consolidate_duplicate_columns(main)
            cleaned_by_col, offenders_after = deep_clean_nonscalars(main, skip_cols=group_cols, verbose=True)
            # 再次确认无重复列
            from collections import Counter
            dup_after = [n for n, c in Counter(main.columns).items() if c > 1]
            if dup_after:
                print(f"  [警告] 合并后仍存在重复列: {dup_after}")
            else:
                print(f"  [校验] 无重复列")
            
            # 执行聚合
            main = main.groupby(group_cols, as_index=False).agg(agg_dict)
            print(f"  After aggregation: {len(main)} rows (grouped by date+platform+id)")
            
            # 调试：聚合后的数据统计
            if '充值金额' in main.columns:
                print(f"  After agg - 充值金额总和: {main['充值金额'].sum():.2f}")
            
            main = main.sort_values(group_cols)
    
    # 只保留 53 列顺序
    main = main[FINAL_COLUMNS]

    # 检查是否有数据
    if len(main) == 0:
        print(f"[ERROR] 清洗后没有任何数据行，跳过生成文件。")
        print(f"   请检查数据源是否包含必要的【日期】和【总代名称/渠道】列。")
        return

    # V3.5.9: 生成缺失字段诊断报告
    print(f"\n[数据诊断] 生成缺失字段报告...")
    try:
        with open("missing_fields_report.txt", "w", encoding="utf-8") as f:
            f.write(f"=== 每日总代数据 - 字段诊断报告 ===\n")
            f.write(f"报表日期: {selected_date if 'selected_date' in locals() else TARGET_DATE}\n")
            f.write(f"总行数: {len(main)}\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for col in FINAL_COLUMNS:
                if col in main.columns:
                    if pd.api.types.is_numeric_dtype(main[col]):
                        non_zero = (main[col] != 0).sum()
                        total_val = main[col].sum()
                        f.write(f"{col}: {non_zero}/{len(main)} 非零 (合计: {total_val:.2f})\n")
                    else:
                        non_empty = main[col].notna().sum()
                        f.write(f"{col}: {non_empty}/{len(main)} 非空\n")
                else:
                    f.write(f"{col}: [缺失列]\n")
        print(f"  ✓ 诊断报告已保存: missing_fields_report.txt")
    except Exception as e:
        print(f"  ⚠️ 诊断报告生成失败: {e}")

    # V3.5.10: 统一浮点数为两位小数
    print(f"\n[格式化] 统一浮点数为两位小数...")
    float_cols = [c for c in main.columns if main[c].dtype == 'float64']
    if float_cols:
        main[float_cols] = main[float_cols].round(2)
        print(f"  ✓ 已格式化 {len(float_cols)} 个浮点列")

    # 写出 Excel
    # V3.1: 添加文件写入异常处理
    write_success = False
    final_output_path = output_path
    
    for attempt in range(3):  # 尝试3次
        try:
            with pd.ExcelWriter(final_output_path, engine="openpyxl") as writer:
                main.to_excel(writer, sheet_name="DailyAgentData", index=False)
            write_success = True
            print(f"✓ 报表生成成功: {len(main)} 行, 53 列")
            print(f"✓ 输出文件: {final_output_path}")
            break
        except PermissionError as e:
            if attempt < 2:  # 前两次尝试使用备份文件名
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_name = os.path.splitext(output_path)[0]
                ext = os.path.splitext(output_path)[1]
                final_output_path = f"{base_name}_backup_{timestamp}{ext}"
                print(f"  ⚠️ 文件被占用，尝试备份文件名: {final_output_path}")
            else:
                print(f"\n❌ 错误：文件写入失败！")
                print(f"   原因：{e}")
                print(f"   解决方案：")
                print(f"   1. 关闭所有打开的Excel文件（特别是 {os.path.basename(output_path)}）")
                print(f"   2. 如果文件仍被占用，请重启Excel或重启电脑")
                print(f"   3. 然后重新运行脚本")
                raise
        except Exception as e:
            print(f"\n❌ 错误：文件写入失败！")
            print(f"   原因：{e}")
            raise
    
    if not write_success:
        print(f"\n❌ 错误：无法写入文件！")
        return
    
    # Save summary to file
    with open("generation_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Report: {os.path.basename(output_path)}\n")
        f.write(f"Rows: {len(main)}\n")
        f.write(f"Columns: {len(main.columns)}\n")
    print("Summary saved to: generation_summary.txt")


if __name__ == "__main__":
    pd.set_option("future.no_silent_downcasting", True)
    parser = argparse.ArgumentParser(description="生成每日总代数据报表")
    parser.add_argument("--input", "-i", default=".", help="输入目录（放置所有数据源的文件夹）")
    parser.add_argument("--output","-o", default=f"每日总代数据_自动生成.xlsx", help="输出Excel路径")
    parser.add_argument("--date", "-d", default=None, 
                       help='目标日期（如 "2025-10-27"）。不指定或使用 "latest" 则自动使用最新日期')
    args = parser.parse_args()
    
    # 使用命令行参数或配置文件中的 TARGET_DATE
    target_date = args.date if args.date else TARGET_DATE
    
    main(args.input, args.output, target_date)
