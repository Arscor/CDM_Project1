import pandas as pd
from pathlib import Path

base_dir = Path(__file__).resolve().parents[1]
data_dir = base_dir / 'data'

df = pd.read_csv(data_dir / 'site_log.csv')

df['event_dt'] = pd.to_datetime(df['event_dt'])

df_start = df[df['event_type'] == 'Query Created'].groupby('query_id')['event_dt'].min().reset_index()
df_start.rename(columns={'event_dt': 'start_time'}, inplace=True)

df_end = df[df['event_type'] == 'DM Closed'].groupby('query_id')['event_dt'].max().reset_index()
df_end.rename(columns={'event_dt': 'end_time'}, inplace=True)

df_site_map = df[['query_id', 'SITEID']].drop_duplicates()

df_cycle = pd.merge(df_start, df_end, on='query_id', how='inner')
df_cycle = pd.merge(df_cycle, df_site_map, on='query_id', how='left')

df_cycle['duration_days'] = (df_cycle['end_time'] - df_cycle['start_time']).dt.total_seconds() / (24 * 3600)

avg_cycle_time = df_cycle.groupby('SITEID')['duration_days'].mean().round(2)

total_queries_per_site = df.groupby('SITEID')['query_id'].nunique()

reissue_events = df[df['event_type'].str.contains('Reissue', case=False, na=False)]
reissue_counts_per_site = reissue_events.groupby('SITEID').size()

reissue_stats = pd.concat([total_queries_per_site, reissue_counts_per_site], axis=1)
reissue_stats.columns = ['total_queries', 'reissue_count']

reissue_stats['reissue_count'] = reissue_stats['reissue_count'].fillna(0)

reissue_stats['reissue_rate'] = (reissue_stats['reissue_count'] / reissue_stats['total_queries']).round(4) # 保留4位小数

final_report = pd.concat([avg_cycle_time, reissue_stats['reissue_rate']], axis=1)
final_report.columns = ['Avg_Cycle_Time(Days)', 'Reissue_Rate']
final_report.index.name = 'SITEID'

final_report['Reissue_Rate_Pct'] = final_report['Reissue_Rate'].apply(lambda x: f"{x*100:.2f}%")

print("------按 Site 聚合统计结果------")
print(final_report)
