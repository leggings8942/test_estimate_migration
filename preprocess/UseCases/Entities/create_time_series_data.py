from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as F

# 日別計測数
def create_time_series_data_daily(df:DataFrame) -> DataFrame:
    # dfに期待する構成
    # root
    #  |-- date:       string (nullable = true)
    #  |-- unique_id:  string (nullable = true)
    #  |-- unique_id2: string (nullable = true)

    # 主に['date', 'unique_id']でレコードを集約する
    df_count = df\
        .select(['date', 'unique_id', 'unique_id2'])\
        .dropDuplicates()\
        .groupBy(['date', 'unique_id'])\
        .agg(F.count('*').alias('daily_count'))
    # df_count.display()
    
    return df_count

# COMMAND ----------

# 時間別計測数
def create_time_series_data_hourly(df:DataFrame) -> DataFrame:
    # dfに期待する構成
    # root
    #  |-- date:       string (nullable = true)
    #  |-- datetime:   string (nullable = true)
    #  |-- unique_id:  string (nullable = true)
    #  |-- unique_id2: string (nullable = true)
    
    # 主に['date', 'hour', 'unique_id']でレコードを集約する
    df_count = df\
            .select(['date', 'datetime', 'unique_id', 'unique_id2'])\
            .withColumn('hour', F.hour('datetime'))\
            .select(['date', 'hour', 'unique_id', 'unique_id2'])\
            .dropDuplicates()\
            .groupBy(['date', 'hour', 'unique_id'])\
            .agg(F.count('*').alias('hourly_count'))
    # df_count.display()
    
    return df_count

# COMMAND ----------

# 30分別計測数
def create_time_series_data_30min(df:DataFrame) -> DataFrame:
    # dfに期待する構成
    # root
    #  |-- date:       string (nullable = true)
    #  |-- datetime:   string (nullable = true)
    #  |-- unique_id:  string (nullable = true)
    #  |-- unique_id2: string (nullable = true)
    
    # 主に['date', 'minute(30分毎)', 'unique_id']でレコードを集約する
    df_count = df\
        .select(['date', 'datetime', 'unique_id', 'unique_id2'])\
        .withColumn('minute', F.date_trunc('minute', 'datetime'))\
        .withColumn('minute', F.unix_timestamp('minute'))\
        .withColumn('minute', col('minute') - (col('minute') % 1800))\
        .withColumn('minute', F.from_unixtime('minute'))\
        .select(['date', 'minute', 'unique_id', 'unique_id2'])\
        .dropDuplicates()\
        .groupBy(['date', 'minute', 'unique_id'])\
        .agg(F.count('*').alias('30min_count'))
    # df_count.display()
    
    return df_count

# COMMAND ----------

# 10分別計測数
def create_time_series_data_10min(df:DataFrame) -> DataFrame:
    # dfに期待する構成
    # root
    #  |-- date:       string (nullable = true)
    #  |-- datetime:   string (nullable = true)
    #  |-- unique_id:  string (nullable = true)
    #  |-- unique_id2: string (nullable = true)
    
    # 主に['date', 'minute(10分毎)', 'unique_id']でレコードを集約する
    df_count = df\
        .select(['date', 'datetime', 'unique_id', 'unique_id2'])\
        .withColumn('minute', F.date_trunc('minute', 'datetime'))\
        .withColumn('minute', F.unix_timestamp('minute'))\
        .withColumn('minute', col('minute') - (col('minute') % 600))\
        .withColumn('minute', F.from_unixtime('minute'))\
        .select(['date', 'minute', 'unique_id', 'unique_id2'])\
        .dropDuplicates()\
        .groupBy(['date', 'minute', 'unique_id'])\
        .agg(F.count('*').alias('10min_count'))
    # df_count.display()
    
    return df_count

# COMMAND ----------

# 1分別計測数
def create_time_series_data_1min(df:DataFrame) -> DataFrame:
    # dfに期待する構成
    # root
    #  |-- date:       string (nullable = true)
    #  |-- datetime:   string (nullable = true)
    #  |-- unique_id:  string (nullable = true)
    #  |-- unique_id2: string (nullable = true)
    
    # 主に['date', 'minute(1分毎)', 'unique_id']でレコードを集約する
    df_count = df\
        .select(['date', 'datetime', 'unique_id', 'unique_id2'])\
        .withColumn('minute', F.date_trunc('minute', 'datetime'))\
        .withColumn('minute', F.unix_timestamp('minute'))\
        .withColumn('minute', col('minute') - (col('minute') % 60))\
        .withColumn('minute', F.from_unixtime('minute'))\
        .select(['date', 'minute', 'unique_id', 'unique_id2'])\
        .dropDuplicates()\
        .groupBy(['date', 'minute', 'unique_id'])\
        .agg(F.count('*').alias('1min_count'))
    # df_count.display()
    
    return df_count
