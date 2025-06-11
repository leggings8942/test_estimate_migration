import glob
import yaml
from pyspark.sql import SparkSession, DataFrame

# pysparkのファイル読み込み時設定について
# inferSchema=True とすると度々変換ミスを起こすことが多発した
# このオプションは有効にするとバグの原因となりうるため、明示的にFalseを指定することとする

def read_csv_file(CSV_PATH:str) -> DataFrame:
    spark = SparkSession.getActiveSession()
    pd_data = spark.read\
                    .option('inferSchema', 'False')\
                    .option('header',      'True')\
                    .csv(CSV_PATH)
    return pd_data

def read_yaml_file(DATABRICKS_PREFIX:str, YAML_PATH:str) -> dict:    
    file_list = glob.glob(DATABRICKS_PREFIX + YAML_PATH)
    if len(file_list) == 0:
        print("folder_path = ", DATABRICKS_PREFIX + YAML_PATH)
        print("Not Exist.")
        raise ValueError("File could not be found.")
    
    with open(file_list[0], 'r') as yml:
        yaml_dic = yaml.safe_load(yml)
    
    return yaml_dic

def read_csv_date_file(INPUT_PATH:str, date:str, p_name:str) -> DataFrame:
    # p_name:処理内容(回遊・人気エリアなど)
    # INPUT_PATH = BASE_PATH + WORK_PATH
    
    spark = SparkSession.getActiveSession()
    path  = INPUT_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/'
    pd_data = spark.read\
                    .option('inferSchema', 'False')\
                    .option('header',      'True')\
                    .csv(path + f'{date}_{p_name}.csv')
    return pd_data

def read_parquet_date_file(PARQUET_PATH:str, date:str) -> DataFrame:
    spark = SparkSession.getActiveSession()
    path  = PARQUET_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/*.parquet'
    parquet_df = spark.read\
                    .option('inferSchema', 'False')\
                    .option('header',      'True')\
                    .parquet(path)
    return parquet_df