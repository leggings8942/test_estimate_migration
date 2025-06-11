import datetime
import pandas as pd


'''
指定日の時系列フレームワークを作成する関数
'''
def create_time_series_by1day(date:str, format='%Y-%m-%d', freq='1min') -> pd.DataFrame:
    # 開始日時と終了日時を設定
    start_time = datetime.datetime.strptime(date, format)
    end_time   = start_time + datetime.timedelta(days=1) - pd.to_timedelta(arg=freq)
    # 指定時間粒度の時間列を作成
    time_range = pd.date_range(start=start_time, end=end_time, freq=freq)
    # DataFrameに変換
    df_time = pd.DataFrame(data=time_range, columns=['datetime'])
    return df_time


'''
指定期間の時系列フレームワークを作成する関数
'''
def create_time_series_by_period(start_date:str, end_date:str, format='%Y-%m-%d %H:%M:%S', freq='1min') -> pd.DataFrame:
    # 開始日時と終了日時を設定
    start_time = datetime.datetime.strptime(start_date, format)
    end_time   = datetime.datetime.strptime(end_date,   format)
    end_time   = end_time - pd.to_timedelta(arg=freq)
    # 指定時間粒度の時間列を作成
    time_range = pd.date_range(start=start_time, end=end_time, freq=freq)
    # DataFrameに変換
    df_time = pd.DataFrame(data=time_range, columns=['datetime'])
    return df_time