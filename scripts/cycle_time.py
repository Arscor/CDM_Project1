import pandas as pd
from pathlib import Path

# 1. 读取数据
base_dir = Path(__file__).resolve().parents[1]
data_dir = base_dir / 'data'

file_path = data_dir / 'query_event_log.csv'
df = pd.read_csv(file_path)

# 2. 转换时间格式
# 确保 event_dt 是 datetime 类型，以便进行数学计算
df['event_dt'] = pd.to_datetime(df['event_dt'])

# 3. 提取 "Query Created" 时间 (Start Time)
# 逻辑：筛选 event_type 为 "Query Created" 的行
# 分组：按 query_id 分组
# 聚合：取 min() 最早时间，代表 Query 产生的时刻
df_created = df[df['event_type'] == 'Query Created'].groupby('query_id')['event_dt'].min().reset_index()
df_created.rename(columns={'event_dt': 'created_dt'}, inplace=True)

# 4. 提取 "DM Closed" 时间 (End Time)
# 逻辑：筛选 event_type 为 "DM Closed" 的行
# 聚合：取 max() 最晚时间，代表 Query 最终关闭的时刻（处理 Re-open 的情况）
df_closed = df[df['event_type'] == 'DM Closed'].groupby('query_id')['event_dt'].max().reset_index()
df_closed.rename(columns={'event_dt': 'closed_dt'}, inplace=True)

# 5. 合并数据 (Inner Join)
# 只保留既有创建记录又有关闭记录的 Query
df_cycle = pd.merge(df_created, df_closed, on='query_id', how='inner')

# 6. 计算时间差
# 计算 timedelta 对象
df_cycle['time_delta'] = df_cycle['closed_dt'] - df_cycle['created_dt']

# 转换为小时 (总秒数 / 3600)
df_cycle['duration_hours'] = df_cycle['time_delta'].dt.total_seconds() / 3600

# 转换为天 (总秒数 / 86400)
df_cycle['duration_days'] = df_cycle['time_delta'].dt.total_seconds() / (3600 * 24)

# 7. 格式化输出
# 保留两位小数，方便查看
df_cycle['duration_hours'] = df_cycle['duration_hours'].round(2)
df_cycle['duration_days'] = df_cycle['duration_days'].round(2)

# 选择最终输出的列
final_output = df_cycle[['query_id', 'created_dt', 'closed_dt', 'duration_hours', 'duration_days']]

# 8. 打印预览并保存
print(f"成功计算 {len(final_output)} 条已关闭 Query 的周期时间。")
print(final_output.head())

# 保存为 CSV
final_output.to_csv(data_dir / 'cycle_time.csv', index=False, encoding='utf-8-sig')
