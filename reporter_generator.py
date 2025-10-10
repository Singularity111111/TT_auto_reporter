import pandas as pd
import os
from datetime import date
import numpy as np
import sys

# ===============================================
#                 ⭐⭐⭐ 配置区 ⭐⭐⭐
# ===============================================

# 1. 基础数据的 Google Sheets 链接
BASE_DATA_URL = "https://docs.google.com/spreadsheets/d/16uAgorJ0Rd6fo7oZCi0_G7y3wEtPxU1eU7A0EXz-rIU/export?format=csv&gid=0"

# 2. 最终生成报表的路径
FINAL_REPORT_PATH = "."  # "." 代表当前文件夹


# ===============================================

def safe_division(numerator, denominator):
    """安全的除法函数，避免除以零的错误"""
    numerator = pd.to_numeric(numerator, errors='coerce').fillna(0)
    denominator = pd.to_numeric(denominator, errors='coerce').fillna(0)
    result = np.divide(numerator, denominator, out=np.zeros_like(numerator, dtype=float), where=denominator != 0)
    return result


# --- 主程序开始 ---
print("--- 报表生成器 (v3.2 - 智能诊断版) ---")

try:
    # --- 步骤 1/3: 加载并智能清洗基础数据 ---
    print(f"\n[步骤 1/3] 正在从网络链接加载基础数据...")
    df_base = pd.read_csv(BASE_DATA_URL)
    
    # ========================= ⭐ 核心升级：打印所有列名用于诊断 ⭐ =========================
    print("  > 成功读取数据，以下是侦测到的所有列名:")
    print("  -------------------------------------------")
    # 为了方便查看，每5个列名换一行
    columns_list = df_base.columns.tolist()
    for i in range(0, len(columns_list), 5):
        print("  " + " | ".join(columns_list[i:i+5]))
    print("  -------------------------------------------")
    # ==============================================================================

    # 统一日期列名
    if '日期Date' in df_base.columns:
        df_base.rename(columns={'日期Date': '日期'}, inplace=True)
        
    # 检查总代为空的数据 - 总代为空是正常的，不应该过滤掉
    total_before_drop = len(df_base)
    # 注释掉原来的过滤逻辑，因为总代为空是正常的
    # df_base.dropna(subset=['总代'], inplace=True)
    total_after_drop = len(df_base)
    print(f"  > 保留所有数据（包括总代为空的）: {total_after_drop} 行")
    
    # 检查10-8数据
    oct_8_after_total_drop = df_base[df_base['日期'].str.contains('2025-10-08', na=False)]
    print(f"  > 当前10-8数据行数: {len(oct_8_after_total_drop)}")
    if len(oct_8_after_total_drop) > 0:
        print(f"  > 10-8数据包含的渠道: {oct_8_after_total_drop['汇总表分类'].unique()}")

    print("  > 正在对日期列进行智能清洗...")
    if '日期' in df_base.columns:
        # 保存原始行数用于调试
        original_rows = len(df_base)
        print(f"    - 清洗前数据行数: {original_rows}")
        
        # 检查10-8数据
        oct_8_before = df_base[df_base['日期'].str.contains('2025-10-08', na=False)]
        print(f"    - 清洗前10-8数据行数: {len(oct_8_before)}")
        
        df_base['日期'] = df_base['日期'].astype(str).str.strip()
        
        # 更精确的日期处理：先过滤掉明显的无效日期
        invalid_dates = df_base['日期'].str.contains('1000-01-01', na=False)
        print(f"    - 发现无效日期(1000-01-01)行数: {invalid_dates.sum()}")
        
        # 直接删除无效日期的行
        df_base = df_base[~invalid_dates].copy()
        print(f"    - 删除无效日期后数据行数: {len(df_base)}")
        
        # 对剩余的有效日期进行转换
        df_base['日期'] = pd.to_datetime(df_base['日期'], errors='coerce')
        
        # 删除转换失败的日期
        df_base.dropna(subset=['日期'], inplace=True)
        df_base['日期'] = df_base['日期'].dt.strftime('%Y-%m-%d')
        
        print(f"    - 清洗后数据行数: {len(df_base)}")
        
        # 检查10-8数据是否还在
        oct_8_after = df_base[df_base['日期'].str.contains('2025-10-08', na=False)]
        print(f"    - 清洗后10-8数据行数: {len(oct_8_after)}")
        
        if len(oct_8_after) > 0:
            print(f"    - 10-8数据包含的渠道: {oct_8_after['汇总表分类'].unique()}")
    else:
        print("\n[❌ 严重错误] 源数据中缺少'日期'或'日期Date'列，无法继续。")
        sys.exit()
    
    print("[✅ 基础数据加载并清洗完成!]")


    # --- 步骤 2/3: 核心计算与格式化 ---
    print("\n[步骤 2/3] 正在严格按照最终规则进行计算与格式化...")

    # 添加调试信息：显示列名翻译前的数据情况
    print("  > 列名翻译前的数据检查:")
    oct_8_before_translate = df_base[df_base['日期'].str.contains('2025-10-08', na=False)]
    print(f"    - 10-8数据行数: {len(oct_8_before_translate)}")
    if len(oct_8_before_translate) > 0:
        print(f"    - 10-8数据的总代字段值: {oct_8_before_translate['总代'].unique()}")
        print(f"    - 10-8数据的汇总表分类: {oct_8_before_translate['汇总表分类'].unique()}")
    
    # 根据你的说明，总代字段实际应该对应汇总表分类
    # 所以我们需要将汇总表分类的值复制到总代字段
    df_base['总代'] = df_base['汇总表分类'].fillna('')
    
    # 先检查实际存在的列名
    print("  > 检查充值相关的列名:")
    recharge_related_cols = [col for col in df_base.columns if '充' in col]
    for col in recharge_related_cols:
        print(f"    - {col}")
    
    column_translator = {
        '总代': '总代号', '渠道Channel': '推广方式_源',
        '消耗Spending': '消耗', '千展成本CPM': '千展成本crm',
        '点击率CTR': '点击率', '注册Register': '注册人数', '首充FTD': '首充人数', '一级首充': '一级首充人数',
        '首日充值金额': '当日首充金额', '当日充提差': '首充当日充提差', '总充值金额': '充值金额'
    }
    
    # 检查是否有充值人数字段，如果没有则创建一个空字段
    if '总充人数' in df_base.columns:
        column_translator['总充人数'] = '充值人数'
    else:
        print("  > 警告: 未找到'总充人数'字段，将创建空的充值人数字段")
        df_base['充值人数'] = 0  # 创建空的充值人数字段
    df_base.rename(columns=column_translator, inplace=True)
    
    # 添加调试信息：显示列名翻译后的数据情况
    print("  > 列名翻译后的数据检查:")
    oct_8_after_translate = df_base[df_base['日期'].str.contains('2025-10-08', na=False)]
    print(f"    - 10-8数据行数: {len(oct_8_after_translate)}")
    if len(oct_8_after_translate) > 0:
        print(f"    - 10-8数据的总代号字段值: {oct_8_after_translate['总代号'].unique()}")
        print(f"    - 10-8数据的汇总表分类: {oct_8_after_translate['汇总表分类'].unique()}")

    print("  > 正在清洗关键数字列...")
    numeric_cols = [
        '消耗', '展示', '点击', '注册人数', '首充人数', '一级首充人数',
        '当日首充金额', '首充当日充提差', '充值金额', '充值人数'
    ]
    for col in numeric_cols:
        if col in df_base.columns:
            # 先用正则表达式移除所有非数字和非小数点的字符
            df_base[col] = df_base[col].astype(str).str.replace(r'[^\d.]', '', regex=True)
            # 对于可能产生的空字符串，在转换前替换为0
            df_base.loc[:, col] = df_base[col].replace('', '0')
            df_base[col] = pd.to_numeric(df_base[col], errors='coerce').fillna(0)


    print("  > 正在按 '日期' 和 '渠道' 对核心数据进行分组汇总...")
    
    # 定义理想的分组依据列名
    GROUPING_COLUMN = '汇总表分类'

    # 检查这个关键列是否存在
    if GROUPING_COLUMN not in df_base.columns:
        print(f"\n[❌ 严重错误] 无法找到用于分组的关键列: '{GROUPING_COLUMN}'")
        print("   请检查上面打印出的列名列表，确认正确的列名是什么，然后联系我们进行修改。")
        sys.exit() # 找不到关键列，直接退出

    grouping_keys = ['日期', GROUPING_COLUMN, '总代号', '部门']
    valid_grouping_keys = [key for key in grouping_keys if key in df_base.columns]

    numeric_cols_to_sum = [
        '消耗', '展示', '点击', '注册人数', '首充人数', '一级首充人数',
        '当日首充金额', '首充当日充提差', '充值金额', '充值人数'
    ]
    
    agg_rules = {}
    for col in numeric_cols_to_sum:
        if col in df_base.columns:
            agg_rules[col] = 'sum'
    
    df_aggregated = df_base.groupby(valid_grouping_keys).agg(agg_rules).reset_index()
    print(f"[✅ 数据汇总完成! 数据从 {len(df_base)} 行聚合为 {len(df_aggregated)} 行。]")
    
    # 检查汇总后的10-8数据
    oct_8_aggregated = df_aggregated[df_aggregated['日期'].str.contains('2025-10-08', na=False)]
    print(f"  > 汇总后10-8数据行数: {len(oct_8_aggregated)}")
    if len(oct_8_aggregated) > 0:
        print(f"  > 汇总后10-8数据包含的渠道: {oct_8_aggregated['汇总表分类'].unique()}")
    
    df_processed = df_aggregated

    def get_col(df, col_name, default_value=''):
        if col_name in df.columns:
            return df[col_name]
        else:
            return pd.Series([default_value] * len(df), index=df.index)

    df_final = pd.DataFrame()

    # --- 按照要求设置字段值 ---
    df_final['产品'] = 'TT'  # 设置为TT
    df_final['盘口'] = get_col(df_processed, GROUPING_COLUMN)
    df_final['日期'] = get_col(df_processed, '日期')
    df_final['总代号'] = ''  # 设置为空白
    df_final['总代名称'] = 'TT'  # 设置为TT
    df_final['推广部门'] = 'A8'  # 统一设置为A8
    df_final['推广方式'] = get_col(df_processed, GROUPING_COLUMN).str.split('-').str[0].fillna('')

    df_final['消耗'] = get_col(df_processed, '消耗', 0)
    df_final['展示'] = get_col(df_processed, '展示', 0)
    df_final['点击'] = get_col(df_processed, '点击', 0)
    df_final['注册人数'] = get_col(df_processed, '注册人数', 0)
    df_final['首充人数'] = get_col(df_processed, '首充人数', 0)
    df_final['一级首充人数'] = get_col(df_processed, '一级首充人数', 0)
    df_final['当日首充金额'] = get_col(df_processed, '当日首充金额', 0)
    df_final['首充当日充提差'] = get_col(df_processed, '首充当日充提差', 0)
    df_final['充值金额'] = get_col(df_processed, '充值金额', 0)
    df_final['充值人数'] = get_col(df_processed, '充值人数', 0)

    df_final['千展成本crm'] = safe_division(df_final['消耗'] * 1000, df_final['展示'])
    df_final['点击率'] = safe_division(df_final['点击'], df_final['展示'])
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
    df_final['自然月消耗'] = ''

    final_column_order = [
        '产品', '盘口', '日期', '总代号', '总代名称', '推广部门', '推广方式', '消耗', '展示', '点击',
        '千展成本crm', '点击率', '注册成本', '首充成本', '一级首充成本', '注册人数', '首充人数', '一级首充人数',
        '首充转化率', '首充arppu', '首充roas', '首充当日ltv', '首充当日roi', '首充充提差比', '当日首充金额',
        '首充当日充提差', '累计roas', '自然月消耗', '非一级首充人数/首充人数', '非一级首充人数/充值人数',
        '充值金额', '充值人数'
    ]
    for col in final_column_order:
        if col not in df_final.columns:
            df_final[col] = ''
    df_final = df_final[final_column_order]

    print("[✅ 计算与格式化完成!]")

    # --- 步骤 3/3: 保存最终报表 ---
    print("\n[步骤 3/3] 正在生成最终Excel报表...")
    today_str = date.today().strftime('%Y-%m-%d')
    
    # 智能文件名生成：如果文件已存在，添加时间戳
    base_filename = f"final_report_{today_str}.xlsx"
    final_filepath = os.path.join(FINAL_REPORT_PATH, base_filename)
    
    # 检查文件是否已存在或被占用
    if os.path.exists(final_filepath):
        import datetime
        timestamp = datetime.datetime.now().strftime('%H%M%S')
        final_filename = f"final_report_{today_str}_{timestamp}.xlsx"
        final_filepath = os.path.join(FINAL_REPORT_PATH, final_filename)
        print(f"  > 检测到同名文件已存在，将保存为新文件: {final_filename}")
    else:
        final_filename = base_filename
    
    try:
        df_final.to_excel(final_filepath, index=False)
    except PermissionError:
        # 如果仍然有权限问题，尝试生成带时间戳的文件
        import datetime
        timestamp = datetime.datetime.now().strftime('%H%M%S')
        final_filename = f"final_report_{today_str}_{timestamp}.xlsx"
        final_filepath = os.path.join(FINAL_REPORT_PATH, final_filename)
        print(f"  > 文件被占用，正在保存为新文件: {final_filename}")
        df_final.to_excel(final_filepath, index=False)

    print("\n" + "=" * 50)
    print("🎉🎉🎉 最终报表生成成功！ 🎉🎉🎉")
    print(f"文件已保存为: {final_filename}")
    print("=" * 50)

except Exception as e:
    print(f"\n[❌ 严重错误] 程序在运行中发生意外: {e}")
