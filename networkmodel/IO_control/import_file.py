import glob
import yaml
import pandas as pd
from pyspark.sql import SparkSession, types

# pysparkのファイル読み込み時設定について
# inferSchema=True とすると度々変換ミスを起こすことが多発した
# このオプションは有効にするとバグの原因となりうるため、明示的にFalseを指定することとする

def read_yaml_file_noSpark(YAML_PATH:str) -> dict:    
    file_list = glob.glob(YAML_PATH)
    if len(file_list) == 0:
        print("folder_path = ", YAML_PATH)
        print("Not Exist.")
        raise ValueError("File could not be found.")

    
    with open(file_list[0], 'r') as yml:
        yml_dic = yaml.safe_load(yml)
    
    return yml_dic

def read_csv_file(CSV_PATH:str, schema:types.StructType | None=None) -> pd.DataFrame:
    spark   = SparkSession.getActiveSession()
    reader  = spark.read.option('header', 'True')
    if schema == None:
        reader = reader.option('inferSchema', 'False')
    else:
        reader = reader.schema(schema=schema)
    
    df_data = reader.csv(CSV_PATH)
    pd_data = df_data.toPandas()
    return pd_data

def read_parquet_file(INPUT_PATH:str, date:str) -> pd.DataFrame:
    folder_path = INPUT_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/*.parquet'
    spark       = SparkSession.getActiveSession()
    df_data     = spark.read\
                        .option('inferSchema', 'False')\
                        .option('header',      'True')\
                        .parquet(folder_path)
    pd_data = df_data.toPandas()
    return pd_data

