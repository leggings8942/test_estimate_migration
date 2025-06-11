from pyspark.sql import SparkSession, DataFrame

# pysparkのファイル読み込み時設定について
# inferSchema=True とすると度々変換ミスを起こすことが多発した
# このオプションは有効にするとバグの原因となりうるため、明示的にFalseを指定することとする

def read_config_file(AIB_PATH:str, CONF_PATH:str) -> tuple[DataFrame, DataFrame]:
    spark = SparkSession.getActiveSession()
    
    aib_nwm_df = spark.read\
                    .option('inferSchema', 'False')\
                    .option('header', 'True')\
                    .csv(AIB_PATH)
    conf_nwm_df = spark.read\
                    .option('inferSchema', 'False')\
                    .option('header', 'True')\
                    .csv(CONF_PATH)
    return aib_nwm_df, conf_nwm_df

def read_csv_file(CSV_PATH:str) -> DataFrame:
    spark  = SparkSession.getActiveSession()
    csv_df = spark.read\
                    .option('inferSchema', 'False')\
                    .option('header', 'True')\
                    .csv(CSV_PATH)
    return csv_df

def read_parquet_date_file(PARQUET_PATH:str, date:str) -> DataFrame:
    spark = SparkSession.getActiveSession()
    path  = PARQUET_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/*.parquet'
    parquet_df = spark.read\
                    .option('inferSchema', 'False')\
                    .option('header', 'True')\
                    .parquet(path)
    return parquet_df