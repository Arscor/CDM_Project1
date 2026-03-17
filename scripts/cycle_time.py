import pandas as pd
from pathlib import Path

base_dir = Path(__file__).resolve().parents[1]
data_dir = base_dir / 'data'

file_path = data_dir / 'query_event_log.csv'
df = pd.read_csv(file_path)

df['event_dt'] = pd.to_datetime(df['event_dt'])

df_created = df[df['event_type'] == 'Query Created'].groupby('query_id')['event_dt'].min().reset_index()
df_created.rename(columns={'event_dt': 'created_dt'}, inplace=True)

df_closed = df[df['event_type'] == 'DM Closed'].groupby('query_id')['event_dt'].max().reset_index()
df_closed.rename(columns={'event_dt': 'closed_dt'}, inplace=True)

df_cycle = pd.merge(df_created, df_closed, on='query_id', how='inner')

df_cycle['time_delta'] = df_cycle['closed_dt'] - df_cycle['created_dt']

df_cycle['duration_hours'] = df_cycle['time_delta'].dt.total_seconds() / 3600

df_cycle['duration_days'] = df_cycle['time_delta'].dt.total_seconds() / (3600 * 24)

df_cycle['duration_hours'] = df_cycle['duration_hours'].round(2)
df_cycle['duration_days'] = df_cycle['duration_days'].round(2)

final_output = df_cycle[['query_id', 'created_dt', 'closed_dt', 'duration_hours', 'duration_days']]

print(f"成功计算 {len(final_output)} 条已关闭 Query 的周期时间。")
print(final_output.head())

final_output.to_csv(data_dir / 'cycle_time.csv', index=False, encoding='utf-8-sig')
