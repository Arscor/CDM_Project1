import pandas as pd
from pathlib import Path

base_dir = Path(__file__).resolve().parents[1]
data_dir = base_dir / 'data'

df_log = pd.read_csv(data_dir / 'query_event_log.csv')

df_raw = pd.read_csv(data_dir / 'raw.csv')

df_site_map = df_raw[['USUBJID', 'SITEID']].drop_duplicates()

df_site_map = df_site_map.dropna(subset=['SITEID'])

df_merged = pd.merge(df_log, df_site_map, on='USUBJID', how='inner')

print(f"合并前 Query 记录数: {len(df_log)}")
print(f"合并后 Query 记录数: {len(df_merged)}")
print("合并后前5行预览：")
print(df_merged[['query_id', 'USUBJID', 'SITEID', 'event_type']].head())

print("\n按 Site 统计的记录数预览:")
print(df_merged['SITEID'].value_counts().head())

df_merged.to_csv(data_dir / 'site_log.csv', index=False, encoding='utf-8-sig')
print(f"\n文件已保存为: {data_dir / 'site_log.csv'}")
