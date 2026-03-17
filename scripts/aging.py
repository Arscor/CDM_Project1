import pandas as pd
from pathlib import Path
from datetime import datetime

base_dir = Path(__file__).resolve().parents[1]
data_dir = base_dir / 'data'

df = pd.read_csv(data_dir / 'query_event_log.csv')

df['event_dt'] = pd.to_datetime(df['event_dt'])

df_start = df.groupby('query_id')['event_dt'].min().reset_index()
df_start.rename(columns={'event_dt': 'creation_date'}, inplace=True)

df_sorted = df.sort_values(by=['query_id', 'event_dt'], ascending=[True, True])

df_latest = df_sorted.drop_duplicates(subset=['query_id'], keep='last')

df_combined = pd.merge(df_latest, df_start, on='query_id', how='left')

exclude_status = ['Closed', 'Cancelled']

open_queries = df_combined[~df_combined['new_status'].isin(exclude_status)].copy()

current_time = pd.Timestamp("2025-08-15 23:59:59")

open_queries['days_open'] = (current_time - open_queries['creation_date']).dt.days

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

columns_to_show = ['query_id', 'USUBJID', 'table', 'field', 'new_status', 'creation_date', 'days_open', 'aging_category', 'note']
final_result = open_queries[columns_to_show].sort_values(by='days_open', ascending=False)

print(f"当前未关闭 Query 总数: {len(final_result)}")
print(final_result.head())

final_result.to_csv(data_dir / 'aging.csv', index=False, encoding='utf-8-sig')
