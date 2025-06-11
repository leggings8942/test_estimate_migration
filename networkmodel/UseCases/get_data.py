import time
import pandas as pd

from .Entities.time_series_data import create_time_series_by1day
from .Entities.time_series_data import create_time_series_by_period
from ._interface import download_to_file


'''
1分間時系列データの取得
'''
def read_by1min(use_dl:download_to_file, ueid_list:list[str]) -> pd.DataFrame:
    df = use_dl.read_parquet_for_1min(ueid_list)
    df = df.rename(columns={'minute': 'datetime'})
    return df


'''
1分間時系列データの整形
'''
# 1分間時系列データの取得
# データ構造を[datetime|udid-A|udid-B|...|udid-N|]とする
def get_time_series_data(use_dl:download_to_file, date:str, ueid_list:list[str]) -> pd.DataFrame:
    
    # 時間計測開始
    time_start = time.perf_counter()
    
    # 1分間時系列データの取得
    df = read_by1min(use_dl, ueid_list)
    
    # 時間計測完了
    time_end  = time.perf_counter()
    time_diff = time_end - time_start
    print(f"{date}:    |- 1分間隔時系列データファイル取得時間 {time_diff}秒")
    
    # データ構造を[datetime|udid-A|udid-B|...|udid-N|]とする
    df_utid   = df.pivot_table(values='1min_count', index='datetime', columns='unique_id', aggfunc='sum', fill_value=0, sort=True)
    
    # pivot_tableは便利であるが、いくつかの点で注意が必要である
    # 1. unique_id列に存在しないデータは、列として存在しない
    # 2. datetime列に抜けがある場合には、出力結果も歯抜けになる
    
    # データとして存在しなかったunique_id列を追加する
    diff_set = set(ueid_list) - set(df_utid.columns)
    for diff in diff_set:
        df_utid[diff] = 0
    
    # unique_id列の順番を整えて、datetime列をindexから列に戻す
    df_utid = df_utid.reindex(columns=sorted(df_utid.columns))
    df_utid = df_utid.reset_index()
    
    # 歯抜けになっている時間軸を補完する
    # その際に歯抜けになっていた部分には0を入れる
    df_by1min = create_time_series_by1day(date, format='%Y-%m-%d', freq='1min')
    df_by1min = pd.merge(df_by1min, df_utid, on='datetime', how='left')
    df_by1min = df_by1min.fillna(0).sort_values(by='datetime', ignore_index=True)
    
    return df_by1min


'''
1分間時系列データの内、指定時刻分のみを抽出する関数
条件として、受け取るDataFrameの扱う期間は1日分とする
'''
def extract_data_by_time(df_1min:pd.DataFrame, date:str, start_time:str, end_time:str) -> pd.DataFrame:
    # 指定期間の時系列データを作成
    start_date = f'{date} {start_time}'
    end_date   = f'{date} {end_time}'
    format     = f'%Y-%m-%d %H:%M:%S'
    df_period  = create_time_series_by_period(start_date=start_date, end_date=end_date, format=format, freq='1min')
    
    # 指定時刻のフィルタリング
    df_filtered = pd.merge(df_period, df_1min, on='datetime', how='left')
    df_filtered = df_filtered.sort_values(by='datetime', ignore_index=True)
    return df_filtered