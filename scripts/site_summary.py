import pandas as pd
from pathlib import Path

# 1. 读取数据 (使用上一生成的文件)
base_dir = Path(__file__).resolve().parents[1]
data_dir = base_dir / 'data'

df = pd.read_csv(data_dir / 'site_log.csv')

# 2. 数据预处理
# 转换时间格式
df['event_dt'] = pd.to_datetime(df['event_dt'])

# ==========================================
# 计算指标 1: 按 Site 聚合的平均开启到关闭时长 (天)
# ==========================================

# 2.1 获取每个 Query 的开始时间 (Query Created)
# 按 query_id 分组找最早时间
df_start = df[df['event_type'] == 'Query Created'].groupby('query_id')['event_dt'].min().reset_index()
df_start.rename(columns={'event_dt': 'start_time'}, inplace=True)

# 2.2 获取每个 Query 的结束时间 (DM Closed)
# 按 query_id 分组找最晚时间 (处理多次开关的情况)
df_end = df[df['event_type'] == 'DM Closed'].groupby('query_id')['event_dt'].max().reset_index()
df_end.rename(columns={'event_dt': 'end_time'}, inplace=True)

# 2.3 获取 Query ID 到 Site 的映射关系 (去重)
df_site_map = df[['query_id', 'SITEID']].drop_duplicates()

# 2.4 合并表：Start + End + Site
# 使用 inner join，只计算那些已经关闭（有 Created 也有 Closed）的 Query
df_cycle = pd.merge(df_start, df_end, on='query_id', how='inner')
df_cycle = pd.merge(df_cycle, df_site_map, on='query_id', how='left')

# 2.5 计算单条 Query 时长 (天)
df_cycle['duration_days'] = (df_cycle['end_time'] - df_cycle['start_time']).dt.total_seconds() / (24 * 3600)

# 2.6 按 Site 聚合计算平均值
avg_cycle_time = df_cycle.groupby('SITEID')['duration_days'].mean().round(2)

# ==========================================
# 计算指标 2: 按 Site 聚合的重发率 (Reissue Rate)
# 公式: 重发事件次数 / 该 Site 的总 Query 数量
# ==========================================

# 3.1 分母：计算每个 Site 的 Query 总数 (去重后的 Query ID 数量)
total_queries_per_site = df.groupby('SITEID')['query_id'].nunique()

# 3.2 分子：计算每个 Site 的 "Reissued" 次数
# 假设 event_type 中包含 'Reissued' 字符串（不区分大小写），或者你可以根据实际情况修改为 == 'Query Reissued'
# 这里的逻辑是筛选出所有代表“重发”的行，然后按 Site 计数
reissue_events = df[df['event_type'].str.contains('Reissue', case=False, na=False)]
reissue_counts_per_site = reissue_events.groupby('SITEID').size()

# 3.3 合并分子分母
# 使用 concat 将两个 Series 合并为 DataFrame
reissue_stats = pd.concat([total_queries_per_site, reissue_counts_per_site], axis=1)
reissue_stats.columns = ['total_queries', 'reissue_count']

# 填充 NaN：如果某个 Site 没有重发记录，reissue_count 会是 NaN，需要填 0
reissue_stats['reissue_count'] = reissue_stats['reissue_count'].fillna(0)

# 3.4 计算比率
reissue_stats['reissue_rate'] = (reissue_stats['reissue_count'] / reissue_stats['total_queries']).round(4) # 保留4位小数

# ==========================================
# 4. 最终合并展示
# ==========================================
final_report = pd.concat([avg_cycle_time, reissue_stats['reissue_rate']], axis=1)
final_report.columns = ['Avg_Cycle_Time(Days)', 'Reissue_Rate']
final_report.index.name = 'SITEID'

# 转换重发率为百分比格式字符串 (可选，方便查看)
final_report['Reissue_Rate_Pct'] = final_report['Reissue_Rate'].apply(lambda x: f"{x*100:.2f}%")

print("------按 Site 聚合统计结果------")
print(final_report)
