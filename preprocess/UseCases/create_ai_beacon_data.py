from pyspark.sql import DataFrame, types
from pyspark.sql.functions import col

from .Entities.create_time_series_data import create_time_series_data_daily
from .Entities.create_time_series_data import create_time_series_data_hourly
from .Entities.create_time_series_data import create_time_series_data_30min
from .Entities.create_time_series_data import create_time_series_data_10min
from .Entities.create_time_series_data import create_time_series_data_1min
from ._interface import upload_to_file


def create_data_daily(spec_comb:upload_to_file, df:DataFrame) -> None:
    # dfに期待する構成
    # root
    #  |-- date:        string  (nullable = true)
    #  |-- folder_name: string  (nullable = true)
    #  |-- user_id:     string  (nullable = true)
    #  |-- place_id:    string  (nullable = true)
    #  |-- floor_name:  string  (nullable = true)
    #  |-- network_id:  string  (nullable = true)
    #  |-- unit_id:     string  (nullable = true)
    #  |-- aibeaconid:  string  (nullable = true)
    
    print('Creating time series data for daily')
    
    # DataFrameを整える
    df_count = df\
                .select(['date', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id', 'aibeaconid'])\
                .withColumnRenamed('unit_id',    'unique_id')\
                .withColumnRenamed('aibeaconid', 'unique_id2')

    df_daily = create_time_series_data_daily(df_count)
    df_count = df_count.select(['folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unique_id']).dropDuplicates()
    
    df_count = df_daily.join(df_count, on='unique_id', how='left')\
                    .withColumnRenamed('unique_id',  'unit_id')\
                    .select( ['date', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id', 'daily_count'])\
                    .orderBy(['date', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id'])
    # df_count.display()
    
    # 各列の型の明示
    df_count = df_count\
                    .withColumn('date',        col('date').cast(       types.StringType()))\
                    .withColumn('folder_name', col('folder_name').cast(types.StringType()))\
                    .withColumn('user_id',     col('user_id').cast(    types.StringType()))\
                    .withColumn('place_id',    col('place_id').cast(   types.StringType()))\
                    .withColumn('floor_name',  col('floor_name').cast( types.StringType()))\
                    .withColumn('network_id',  col('network_id').cast( types.StringType()))\
                    .withColumn('unit_id',     col('unit_id').cast(    types.StringType()))\
                    .withColumn('daily_count', col('daily_count').cast(types.LongType()))
    
    spec_comb.write_parquet_date_file('daily/', df_count)



def create_data_hourly(spec_comb:upload_to_file, df:DataFrame) -> None:
    # dfに期待する構成
    # root
    #  |-- date:        string  (nullable = true)
    #  |-- datetime:    string  (nullable = true)
    #  |-- folder_name: string  (nullable = true)
    #  |-- user_id:     string  (nullable = true)
    #  |-- place_id:    string  (nullable = true)
    #  |-- floor_name:  string  (nullable = true)
    #  |-- network_id:  string  (nullable = true)
    #  |-- unit_id:     string  (nullable = true)
    #  |-- aibeaconid:  string  (nullable = true)
    
    print('Creating time series data for hourly')
    
    # DataFrameを整える
    df_count = df\
                .select(['date', 'datetime', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id', 'aibeaconid'])\
                .withColumnRenamed('unit_id',    'unique_id')\
                .withColumnRenamed('aibeaconid', 'unique_id2')

    df_hourly = create_time_series_data_hourly(df_count)
    df_count  = df_count.select(['folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unique_id']).dropDuplicates()
    
    df_count  = df_hourly.join(df_count, on='unique_id', how='left')\
                    .withColumnRenamed('unique_id',  'unit_id')\
                    .select( ['date', 'hour', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id', 'hourly_count'])\
                    .orderBy(['date', 'hour', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id'])
    # df_count.display()
    
    # 各列の型の明示
    df_count = df_count\
                    .withColumn('date',         col('date').cast(        types.StringType()))\
                    .withColumn('hour',         col('hour').cast(        types.StringType()))\
                    .withColumn('folder_name',  col('folder_name').cast( types.StringType()))\
                    .withColumn('user_id',      col('user_id').cast(     types.StringType()))\
                    .withColumn('place_id',     col('place_id').cast(    types.StringType()))\
                    .withColumn('floor_name',   col('floor_name').cast(  types.StringType()))\
                    .withColumn('network_id',   col('network_id').cast(  types.StringType()))\
                    .withColumn('unit_id',      col('unit_id').cast(     types.StringType()))\
                    .withColumn('hourly_count', col('hourly_count').cast(types.LongType()))
    
    spec_comb.write_parquet_date_file('hourly/', df_count)



def create_data_30min(spec_comb:upload_to_file, df:DataFrame) -> None:
    # dfに期待する構成
    # root
    #  |-- date:        string  (nullable = true)
    #  |-- datetime:    string  (nullable = true)
    #  |-- folder_name: string  (nullable = true)
    #  |-- user_id:     string  (nullable = true)
    #  |-- place_id:    string  (nullable = true)
    #  |-- floor_name:  string  (nullable = true)
    #  |-- network_id:  string  (nullable = true)
    #  |-- unit_id:     string  (nullable = true)
    #  |-- aibeaconid:  string  (nullable = true)
    
    
    print('Creating time series data for 30min')
    
    # DataFrameを整える
    df_count = df\
                .select(['date', 'datetime', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id', 'aibeaconid'])\
                .withColumnRenamed('unit_id',    'unique_id')\
                .withColumnRenamed('aibeaconid', 'unique_id2')
    
    df_30min = create_time_series_data_30min(df_count)
    df_count = df_count.select(['folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unique_id']).dropDuplicates()
    
    df_count = df_30min.join(df_count, on='unique_id', how='left')\
                    .withColumnRenamed('unique_id',  'unit_id')\
                    .select( ['date', 'minute', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id', '30min_count'])\
                    .orderBy(['date', 'minute', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id'])
    # df_count.display()
    
    # 各列の型の明示
    df_count = df_count\
                    .withColumn('date',        col('date').cast(       types.StringType()))\
                    .withColumn('minute',      col('minute').cast(     types.StringType()))\
                    .withColumn('folder_name', col('folder_name').cast(types.StringType()))\
                    .withColumn('user_id',     col('user_id').cast(    types.StringType()))\
                    .withColumn('place_id',    col('place_id').cast(   types.StringType()))\
                    .withColumn('floor_name',  col('floor_name').cast( types.StringType()))\
                    .withColumn('network_id',  col('network_id').cast( types.StringType()))\
                    .withColumn('unit_id',     col('unit_id').cast(    types.StringType()))\
                    .withColumn('30min_count', col('30min_count').cast(types.LongType()))
    
    spec_comb.write_parquet_date_file('by30min/', df_count)



def create_data_10min(spec_comb:upload_to_file, df:DataFrame) -> None:
    # dfに期待する構成
    # root
    #  |-- date:        string  (nullable = true)
    #  |-- datetime:    string  (nullable = true)
    #  |-- folder_name: string  (nullable = true)
    #  |-- user_id:     string  (nullable = true)
    #  |-- place_id:    string  (nullable = true)
    #  |-- floor_name:  string  (nullable = true)
    #  |-- network_id:  string  (nullable = true)
    #  |-- unit_id:     string  (nullable = true)
    #  |-- aibeaconid:  string  (nullable = true)
    
    print('Creating time series data for 10min')
    
    # DataFrameを整える
    df_count = df\
                .select(['date', 'datetime', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id', 'aibeaconid'])\
                .withColumnRenamed('unit_id',    'unique_id')\
                .withColumnRenamed('aibeaconid', 'unique_id2')
    
    df_10min = create_time_series_data_10min(df_count)
    df_count = df_count.select(['folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unique_id']).dropDuplicates()
    
    df_count = df_10min.join(df_count, on='unique_id', how='left')\
                    .withColumnRenamed('unique_id',  'unit_id')\
                    .select( ['date', 'minute', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id', '10min_count'])\
                    .orderBy(['date', 'minute', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id'])
    # df_count.display()
    
    # 各列の型の明示
    df_count = df_count\
                    .withColumn('date',        col('date').cast(       types.StringType()))\
                    .withColumn('minute',      col('minute').cast(     types.StringType()))\
                    .withColumn('folder_name', col('folder_name').cast(types.StringType()))\
                    .withColumn('user_id',     col('user_id').cast(    types.StringType()))\
                    .withColumn('place_id',    col('place_id').cast(   types.StringType()))\
                    .withColumn('floor_name',  col('floor_name').cast( types.StringType()))\
                    .withColumn('network_id',  col('network_id').cast( types.StringType()))\
                    .withColumn('unit_id',     col('unit_id').cast(    types.StringType()))\
                    .withColumn('10min_count', col('10min_count').cast(types.LongType()))
    
    spec_comb.write_parquet_date_file('by10min/', df_count)



def create_data_1min(spec_comb:upload_to_file, df:DataFrame) -> None:
    # dfに期待する構成
    # root
    #  |-- date:        string  (nullable = true)
    #  |-- datetime:    string  (nullable = true)
    #  |-- folder_name: string  (nullable = true)
    #  |-- user_id:     string  (nullable = true)
    #  |-- place_id:    string  (nullable = true)
    #  |-- floor_name:  string  (nullable = true)
    #  |-- network_id:  string  (nullable = true)
    #  |-- unit_id:     string  (nullable = true)
    #  |-- aibeaconid:  string  (nullable = true)
    
    print('Creating time series data for 1min')
    
    # DataFrameを整える
    df_count = df\
                .select(['date', 'datetime', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id', 'aibeaconid'])\
                .withColumnRenamed('unit_id',    'unique_id')\
                .withColumnRenamed('aibeaconid', 'unique_id2')
    
    df_1min = create_time_series_data_1min(df_count)
    df_count = df_count.select(['folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unique_id']).dropDuplicates()
    
    df_count = df_1min.join(df_count, on='unique_id', how='left')\
                    .withColumnRenamed('unique_id',  'unit_id')\
                    .select( ['date', 'minute', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id', '1min_count'])\
                    .orderBy(['date', 'minute', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id'])
    # df_count.display()
    
    # 各列の型の明示
    df_count = df_count\
                    .withColumn('date',        col('date').cast(       types.StringType()))\
                    .withColumn('minute',      col('minute').cast(     types.StringType()))\
                    .withColumn('folder_name', col('folder_name').cast(types.StringType()))\
                    .withColumn('user_id',     col('user_id').cast(    types.StringType()))\
                    .withColumn('place_id',    col('place_id').cast(   types.StringType()))\
                    .withColumn('floor_name',  col('floor_name').cast( types.StringType()))\
                    .withColumn('network_id',  col('network_id').cast( types.StringType()))\
                    .withColumn('unit_id',     col('unit_id').cast(    types.StringType()))\
                    .withColumn('1min_count',  col('1min_count').cast( types.LongType()))
    
    spec_comb.write_parquet_date_file('by1min/', df_count)
    


def create_ai_beacon_data(spec_comb:upload_to_file, df:DataFrame) -> None:
    create_data_daily( spec_comb, df) # 1日単位でデータを集計し保存する
    create_data_hourly(spec_comb, df) # 1時間単位でデータを集計し保存する
    create_data_1min(  spec_comb, df) # 1分単位でデータを集計し保存する

