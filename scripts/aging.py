import pandas as pd
from pathlib import Path
from datetime import datetime

# 1. 读取数据
base_dir = Path(__file__).resolve().parents[1]
data_dir = base_dir / 'data'

df = pd.read_csv(data_dir / 'query_event_log.csv')

# 2. 数据预处理：将时间字段转换为 datetime 格式
# 如果csv中有中文导致乱码，可以在 read_csv 中添加 encoding='utf-8-sig' 或 'gbk'
df['event_dt'] = pd.to_datetime(df['event_dt'])

# 3. 获取每个 Query 的关键信息
# ---------------------------------------------------------
# 逻辑：
# Start Time (创建时间): 每个 query_id 最早的一条记录的时间
# Current Status (当前状态): 每个 query_id 时间最晚的一条记录的 new_status
# ---------------------------------------------------------

# 3.1 获取创建时间 (按 query_id 分组取 event_dt 的最小值)
df_start = df.groupby('query_id')['event_dt'].min().reset_index()
df_start.rename(columns={'event_dt': 'creation_date'}, inplace=True)

# 3.2 获取当前最新状态
# 先按 query_id 和 event_dt 排序，确保最后一行是最新的
df_sorted = df.sort_values(by=['query_id', 'event_dt'], ascending=[True, True])
# 取每个 query_id 的最后一行记录保留所有字段信息
df_latest = df_sorted.drop_duplicates(subset=['query_id'], keep='last')

# 4. 合并数据：将“创建时间”合并到“最新状态”表中
df_combined = pd.merge(df_latest, df_start, on='query_id', how='left')

# 5. 筛选未关闭的 Query
# ---------------------------------------------------------
# 排除 new_status 为 'Closed' 或 'Cancelled' 的记录
# 如果 'Cancelled' 出现在 new_status 列中，用以下代码排除：
exclude_status = ['Closed', 'Cancelled']

# 这里的 ~ 表示“非”，即保留不在排除列表中的状态
open_queries = df_combined[~df_combined['new_status'].isin(exclude_status)].copy()

# 6. 计算由创建至今的天数
# 使用 pd.Timestamp.now() 获取当前时间
current_time = pd.Timestamp("2025-08-15 23:59:59")

# 计算时间差，并提取天数 (.dt.days)
open_queries['days_open'] = (current_time - open_queries['creation_date']).dt.days

# 7. 对时间进行分类 (>7, >14, >30)
# ---------------------------------------------------------
def categorize_age(days):
    if days > 30:
        return '>30 Days'
    elif days > 14:
        return '>14 Days'
    elif days > 7:
        return '>7 Days'
    else:
        return '<=7 Days'

open_queries['aging_category'] = open_queries['days_open'].apply(categorize_age)

# 8. 输出结果
columns_to_show = ['query_id', 'USUBJID', 'table', 'field', 'new_status', 'creation_date', 'days_open', 'aging_category', 'note']
final_result = open_queries[columns_to_show].sort_values(by='days_open', ascending=False)

# 打印预览
print(f"当前未关闭 Query 总数: {len(final_result)}")
print(final_result.head())

# 保存为新的 CSV
final_result.to_csv(data_dir / 'aging.csv', index=False, encoding='utf-8-sig')
