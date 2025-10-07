import pandas as pd
import os
from datetime import date
import glob
import numpy as np
import re  # 引入正则表达式库来解析百分比

# ===============================================
#                 ⭐⭐⭐ 配置区 ⭐⭐⭐
# ===============================================

# 1. 基础数据的 Google Sheets 链接
BASE_DATA_URL = "https://docs.google.com/spreadsheets/d/16uAgorJ0Rd6fo7oZCi0_G7y3wEtPxU1eU7A0EXz-rIU/export?format=csv&gid=0"

# 2. 爬虫数据所在的文件夹路径
#    [最终修正] 改为在当前脚本所在的文件夹中直接查找
CRAWLED_DATA_PATH = os.getcwd()

# 3. 最终生成报表的路径
FINAL_REPORT_PATH = "."  # "." 代表当前文件夹


# ===============================================

def find_latest_crawled_file(path, report_name_keyword):
    """根据关键词查找最新生成的爬虫数据文件 (不限日期)"""
    # 搜索模式：在指定路径下，查找所有包含关键词的.csv文件
    search_pattern = os.path.join(path, f"*{report_name_keyword}*.csv")
    files = glob.glob(search_pattern)

    if not files:
        print(f"[⚠️  警告] 在文件夹 '{path}' 中未找到任何与'{report_name_keyword}'相关的文件！将跳过合并。")
        return None

    # 返回找到的文件中，修改时间最新的那一个
    return max(files, key=os.path.getctime)


def extract_percentage(value):
    """从 '3 (75.0%)' 或 '75.0%' 这样的字符串中提取百分比数值"""
    if not isinstance(value, str):
        return 0.0
    # 正则表达式查找括号内或独立的百分比数字
    match = re.search(r'\(?(\d+\.?\d*)%\)?', value)
    if match:
        try:
            return float(match.group(1)) / 100.0
        except (ValueError, IndexError):
            return 0.0
    return 0.0


def process_retention_data(filepath, column_mapping, date_col='日期'):
    """读取、清洗、解析百分比并格式化留存数据"""
    if filepath is None: return None
    print(f"  > 正在处理文件: {os.path.basename(filepath)}")
    df = pd.read_csv(filepath)

    # 应用百分比解析函数到所有需要转换的列
    for col in column_mapping.keys():
        if col in df.columns:
            df[col] = df[col].apply(extract_percentage)

    df.rename(columns=column_mapping, inplace=True)
    df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
    final_cols = [date_col] + list(column_mapping.values())

    existing_cols = [col for col in final_cols if col in df.columns]
    return df[existing_cols]


def safe_division(numerator, denominator):
    """安全的除法函数，避免除以零的错误"""
    numerator = pd.to_numeric(numerator, errors='coerce').fillna(0)
    denominator = pd.to_numeric(denominator, errors='coerce').fillna(0)
    result = np.divide(numerator, denominator, out=np.zeros_like(numerator, dtype=float), where=denominator != 0)
    return result


# --- 主程序开始 ---
print("--- 报表生成器 (v2.7 - 更新核心字段逻辑) ---")

try:
    # --- 步骤 1: 加载并清洗基础数据 ---
    print(f"\n[步骤 1/5] 正在从网络链接加载基础数据...")
    df_base = pd.read_csv(BASE_DATA_URL)

    if '日期Date' in df_base.columns:
        df_base.rename(columns={'日期Date': '日期'}, inplace=True)
    df_base.dropna(subset=['日期'], inplace=True)
    df_base['日期'] = pd.to_datetime(df_base['日期'], errors='coerce')
    df_base.dropna(subset=['日期'], inplace=True)
    df_base['日期'] = df_base['日期'].dt.strftime('%Y-%m-%d')
    print("[✅ 基础数据加载并清洗完成!]")

    # --- 步骤 2: 加载并处理爬虫数据 ---
    print("\n[步骤 2/5] 正在加载并处理爬虫抓取的留存数据...")
    login_retention_cols = {'2日留存': '首充次日复登率_偏移', '3日留存': '首充三日复登率_偏移',
                            '7日留存': '首充七日复登率_偏移', '15日留存': '首充十五日复登率_偏移',
                            '30日留存': '首充三十日复登率_偏移'}
    play_retention_cols = {'2日留存': '首充次日复投率_偏移', '3日留存': '首充三日复投率_偏移',
                           '7日留存': '首充七日复投率_偏移', '15日留存': '首充十五日复投率_偏移',
                           '30日留存': '首充三十日复投率_偏移'}
    pay_retention_cols = {'2日留存': '首充次日复充率_偏移', '3日留存': '首充三日复充率_偏移',
                          '7日留存': '首充七日复充率_偏移', '15日留存': '首充十五日复充率_偏移',
                          '30日留存': '首充三十日复充率_偏移'}

    df_login = process_retention_data(find_latest_crawled_file(CRAWLED_DATA_PATH, "首充登录留存"), login_retention_cols)
    df_play = process_retention_data(find_latest_crawled_file(CRAWLED_DATA_PATH, "首充下注留存"), play_retention_cols)
    df_pay = process_retention_data(find_latest_crawled_file(CRAWLED_DATA_PATH, "首充付费留存"), pay_retention_cols)
    print("[✅ 爬虫数据处理完成!]")

    # --- 步骤 3: 合并所有数据源 ---
    print("\n[步骤 3/5] 正在合并所有数据源...")
    df_merged = df_base
    if df_login is not None: df_merged = pd.merge(df_merged, df_login, on='日期', how='left')
    if df_play is not None: df_merged = pd.merge(df_merged, df_play, on='日期', how='left')
    if df_pay is not None: df_merged = pd.merge(df_merged, df_pay, on='日期', how='left')
    print("[✅ 数据合并成功!]")

    # --- 步骤 4: 核心计算与格式化 ---
    print("\n[步骤 4/5] 正在严格按照最终规则进行计算与格式化...")

    column_translator = {
        '总代': '总代号', '渠道Channel': '推广方式_源',  # 保留原始推广方式，以备后用
        '消耗Spending': '消耗', '千展成本CPM': '千展成本crm',
        '点击率CTR': '点击率', '注册Register': '注册人数', '首充FTD': '首充人数', '一级首充': '一级首充人数',
        '首日充值金额': '当日首充金额', '当日充提差': '首充当日充提差', '总充值金额': '充值金额', '总充人数': '充值人数'
    }
    df_merged.rename(columns=column_translator, inplace=True)

    print("  > 正在清洗关键数字列...")
    numeric_cols = [
        '消耗', '展示', '点击', '千展成本crm', '注册人数', '首充人数', '一级首充人数',
        '当日首充金额', '首充当日充提差', '充值金额', '充值人数'
    ]
    for col in numeric_cols:
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].astype(str).str.replace(r'[^\d.]', '', regex=True)
            df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce').fillna(0)


    def get_col(df, col_name, default_value=''):
        if col_name in df.columns:
            return df[col_name]
        else:
            print(f"[⚠️ 警告] 源数据/合并数据缺少 '{col_name}' 列，将使用默认值填充。")
            return pd.Series([default_value] * len(df), index=df.index)


    df_final = pd.DataFrame()

    # 1. 映射或固定值 (⭐ 应用最新业务逻辑)
    df_final['产品'] = 'TT产品'
    df_final['盘口'] = get_col(df_merged, '汇总表分类')
    df_final['日期'] = get_col(df_merged, '日期')
    df_final['总代号'] = get_col(df_merged, '总代号')
    df_final['总代名称'] = get_col(df_merged, '汇总表分类')
    df_final['推广部门'] = get_col(df_merged, '部门')
    # 从'汇总表分类'提取'-'前的内容作为推广方式
    df_final['推广方式'] = get_col(df_merged, '汇总表分类').str.split('-').str[0].fillna('')

    df_final['消耗'] = get_col(df_merged, '消耗', 0)
    df_final['展示'] = get_col(df_merged, '展示', 0)
    df_final['点击'] = get_col(df_merged, '点击', 0)
    df_final['千展成本crm'] = get_col(df_merged, '千展成本crm', 0)
    df_final['点击率'] = get_col(df_merged, '点击率', 0)
    df_final['注册人数'] = get_col(df_merged, '注册人数', 0)
    df_final['首充人数'] = get_col(df_merged, '首充人数', 0)
    df_final['一级首充人数'] = get_col(df_merged, '一级首充人数', 0)
    df_final['当日首充金额'] = get_col(df_merged, '当日首充金额', 0)
    df_final['首充当日充提差'] = get_col(df_merged, '首充当日充提差', 0)
    df_final['充值金额'] = get_col(df_merged, '充值金额', 0)
    df_final['充值人数'] = get_col(df_merged, '充值人数', 0)

    # 2. 核心计算
    df_final['注册成本'] = safe_division(df_final['消耗'], df_final['注册人数'])
    df_final['首充成本'] = safe_division(df_final['消耗'], df_final['首充人数'])
    df_final['一级首充成本'] = safe_division(df_final['消耗'], df_final['一级首充人数'])
    df_final['首充转化率'] = safe_division(df_final['首充人数'], df_final['注册人数'])
    df_final['首充arppu'] = safe_division(df_final['当日首充金额'], df_final['首充人数'])
    df_final['首充roas'] = safe_division(df_final['当日首充金额'], df_final['消耗'])
    df_final['首充当日ltv'] = safe_division(df_final['首充当日充提差'], df_final['首充人数'])
    df_final['首充当日roi'] = safe_division(df_final['首充当日充提差'], df_final['消耗'])
    df_final['首充充提差比'] = safe_division(df_final['首充当日充提差'], df_final['当日首充金额'])
    df_final['累计roas'] = safe_division(df_final['充值金额'], df_final['消耗'])

    non_primary_ftd = pd.to_numeric(df_final['首充人数'], errors='coerce').fillna(0) - pd.to_numeric(
        df_final['一级首充人数'], errors='coerce').fillna(0)
    df_final['非一级首充人数/首充人数'] = safe_division(non_primary_ftd, df_final['首充人数'])
    df_final['非一级首充人数/充值人数'] = safe_division(non_primary_ftd, df_final['充值人数'])

    # 3. 填充爬虫数据列
    retention_columns = list(login_retention_cols.values()) + list(play_retention_cols.values()) + list(
        pay_retention_cols.values())
    for col in retention_columns:
        df_final[col] = get_col(df_merged, col, None)

    # 4. 增加自然月消耗（留空）
    df_final['自然月消耗'] = ''

    # 5. 确保最终列顺序和数量完全符合要求
    final_column_order = [
        '产品', '盘口', '日期', '总代号', '总代名称', '推广部门', '推广方式', '消耗', '展示', '点击',
        '千展成本crm', '点击率', '注册成本', '首充成本', '一级首充成本', '注册人数', '首充人数', '一级首充人数',
        '首充转化率', '首充arppu', '首充roas', '首充当日ltv', '首充当日roi', '首充充提差比', '当日首充金额',
        '首充当日充提差', '首充次日复登率_偏移', '首充三日复登率_偏移', '首充七日复登率_偏移', '首充十五日复登率_偏移',
        '首充三十日复登率_偏移', '首充次日复投率_偏移', '首充三日复投率_偏移', '首充七日复投率_偏移',
        '首充十五日复投率_偏移',
        '首充三十日复投率_偏移', '首充次日复充率_偏移', '首充三日复充率_偏移', '首充七日复充率_偏移',
        '首充十五日复充率_偏移',
        '首充三十日复充率_偏移', '累计roas', '自然月消耗', '非一级首充人数/首充人数', '非一级首充人数/充值人数',
        '充值金额', '充值人数'
    ]
    df_final = df_final.reindex(columns=final_column_order)

    print("[✅ 计算与格式化完成!]")

    # --- 步骤 5: 保存最终报表 ---
    print("\n[步骤 5/5] 正在生成最终Excel报表...")
    today_str = date.today().strftime('%Y-%m-%d')
    final_filename = f"final_report_{today_str}.xlsx"
    final_filepath = os.path.join(FINAL_REPORT_PATH, final_filename)
    df_final.to_excel(final_filepath, index=False)

    print("\n" + "=" * 50)
    print("🎉🎉🎉 最终报表生成成功！ 🎉🎉🎉")
    print(f"文件已保存为: {final_filename}")
    print("=" * 50)

except Exception as e:
    print(f"\n[❌ 严重错误] 程序在运行中发生意外: {e}")

