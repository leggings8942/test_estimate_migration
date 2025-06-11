import os
from pyspark.sql import DataFrame


def write_parquet_date_file(PREFIX:str, OUTPUT_PATH:str, date:str, ps_df:DataFrame) -> None:    
    # 出力パス
    path = OUTPUT_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/'
    # パス先がなければディレクトリの作成
    if not os.path.isdir(PREFIX + path): 
        os.makedirs(PREFIX + path, exist_ok=True)
    # 出力
    ps_df\
        .coalesce(1)\
        .write\
        .mode('overwrite')\
        .option('header', 'True')\
        .option('compression', 'snappy')\
        .parquet(path)
    
