import pandas as pd
from pathlib import Path

# 1. 读取数据
# 假设 query_event_log.csv 是你之前处理好的或者原始的 query 数据
base_dir = Path(__file__).resolve().parents[1]
data_dir = base_dir / 'data'

df_log = pd.read_csv(data_dir / 'query_event_log.csv')

# 读取包含 SITEID 的 raw 数据
df_raw = pd.read_csv(data_dir / 'raw.csv')

# 2. 准备受试者与中心的映射关系表
# 逻辑：
# - 只保留 'USUBJID' 和 'SITEID' 两列
# - 使用 drop_duplicates()：确保每个 USUBJID 只对应一行记录。
#   如果 raw.csv 包含多个访视(Visit)，不去重会导致合并后 Query 记录翻倍，这是非常重要的一步。
df_site_map = df_raw[['USUBJID', 'SITEID']].drop_duplicates()

# 检查一下 raw 数据中是否有 SITEID 为空的情况，将其剔除
df_site_map = df_site_map.dropna(subset=['SITEID'])

# 3. 合并表格
# 逻辑：
# - on='USUBJID': 以受试者编号为钥匙进行连接
# - how='inner': 内连接。只有在两个表中都存在的受试者才会被保留。
#   这直接满足了你“不要有空值”的要求。如果 Query Log 里有病人不在 raw 表中，会被剔除。
df_merged = pd.merge(df_log, df_site_map, on='USUBJID', how='inner')

# 4. 验证数据（可选）
print(f"合并前 Query 记录数: {len(df_log)}")
print(f"合并后 Query 记录数: {len(df_merged)}")
print("合并后前5行预览：")
print(df_merged[['query_id', 'USUBJID', 'SITEID', 'event_type']].head())

# 简单的聚合统计预览（按 SITEID 统计 Query 数量）
print("\n按 Site 统计的记录数预览:")
print(df_merged['SITEID'].value_counts().head())

# 5. 保存结果到 CSV
df_merged.to_csv(data_dir / 'site_log.csv', index=False, encoding='utf-8-sig')
print(f"\n文件已保存为: {data_dir / 'site_log.csv'}")
