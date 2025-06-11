# Databricks notebook source
# MAGIC %md
# MAGIC #### ver2.1からver2.2への変更内容
# MAGIC - 対象案件ごとに設定されたRSSIの値を基準値として使用する
# MAGIC - RAWデータの中間ファイルの出力を廃止
# MAGIC - データソースを以下の２つのテーブルから選択できるように変更
# MAGIC     - adinte.aibeacon_wifi_log
# MAGIC     - adinte_analyzed_data.gps_contact
# MAGIC - 出力パスと出力ファイルの変更
# MAGIC     - 日別・1時間別・30min別・10min別・1min別の出力ファイルを生成するにあたり、できる限り出力ファイルを<br>
# MAGIC       まとめるように変更した。
# MAGIC     - 各種時間別の出力ファイル<br>
# MAGIC >       silver/{folder_name}/preprocess/daily/year={%Y}/month={%Y-%m}/date={%Y-%m-%d}/{file_name}.snappy.parquet
# MAGIC >       silver/{folder_name}/preprocess/hourly/year={%Y}/month={%Y-%m}/date={%Y-%m-%d}/{file_name}.snappy.parquet
# MAGIC >       silver/{folder_name}/preprocess/by30min/year={%Y}/month={%Y-%m}/date={%Y-%m-%d}/{file_name}.snappy.parquet
# MAGIC >       silver/{folder_name}/preprocess/by10min/year={%Y}/month={%Y-%m}/date={%Y-%m-%d}/{file_name}.snappy.parquet
# MAGIC >       silver/{folder_name}/preprocess/by1min/year={%Y}/month={%Y-%m}/date={%Y-%m-%d}/{file_name}.snappy.parquet

# COMMAND ----------
%pip install tqdm==4.67.1

# COMMAND ----------
import json
import yaml
import datetime
from typing import Literal
from tqdm.autonotebook import tqdm

import pyspark as ps
from pyspark import StorageLevel
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col
import pyspark.sql.functions as F

from IO_control.import_file import read_csv_file
from UseCases.create_ai_beacon_data import create_ai_beacon_data
from _injector import original_UL


# COMMAND ----------
spark = SparkSession.builder\
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")\
            .config("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")\
            .config("spark.sql.adaptive.enabled", True)\
            .config("spark.sql.dynamicPartitionPruning.enabled", True)\
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1MB")\
            .getOrCreate()
            # '_started'と'_committed_'で始まるファイルを書き込まないように設定
            # '_SUCCESS'で始まるファイルを書き込まないように設定
            # AQE(Adaptive Query Execution)の有効化
            # 動的パーティションプルーニングの有効化
            # シャッフル後の1パーティションあたりの最小サイズを指定
# COMMAND ----------
def get_yaml_dict(path:str) -> dict:
    try:
        with open(path, 'r') as yml:
            yaml_dic = yaml.safe_load(yml)
    except Exception as e:
        print("エラーが発生しました::", e)
        print(f"yamlファイルの読み込みに失敗しました。")
        raise FileNotFoundError("file not found.")
    
    return yaml_dic

def get_widgets_var(key:str) -> str:
    try:
        var = dbutils.widgets.get(key)
    except Exception as e:
        print("エラーが発生しました::", e)
        print(f"dbutils.widgets経由での環境変数'{key}' の取得に失敗しました")
        print(f"yaml経由での環境変数'{key}' の取得に切り替えます。")
        conf = get_yaml_dict('../config/basic_config.yaml')
        var  = str(conf[key])
    finally:
        if var is None:
            raise ValueError(f"環境変数'{key}' が設定されていません")
    return var

def get_taskValues_json(taskKey:str, key:str) -> dict:
    try:
        task_json = dbutils.jobs.taskValues.get(taskKey=taskKey, key=key)
    except Exception as e:
        print("エラーが発生しました::", e)
        print(f"dbutils.jobs.taskValues経由での環境変数'{taskKey}.{key}' の取得に失敗しました")
        print(f"dbutils.widgets経由での環境変数'{key}' の取得に切り替えます。")
        task_dict = {}
        task_dict["PROJECT_NAME"]     = get_widgets_var("PROJECT_NAME")
        task_dict["SPECIFIED_PERIOD"] = get_widgets_var("SPECIFIED_PERIOD")
    else:
        task_dict = json.loads(task_json)
    return task_dict

# COMMAND ----------
# databricksのウィジット・ジョブからの設定の読み込み
SETTING_TASK_NAME = get_widgets_var("SETTING_TASK_NAME")
period_json       = get_taskValues_json(SETTING_TASK_NAME, 'target_period')

PROJECT_NAME      = period_json["PROJECT_NAME"]
SPECIFIED_PERIOD  = period_json["SPECIFIED_PERIOD"]

# COMMAND ----------
# 基本設定ファイルの読み込み
bs_conf_dic = get_yaml_dict('../config/basic_config.yaml')
print(bs_conf_dic)

# 基本設定を取得する
BASE_PATH            = bs_conf_dic["BASE_PATH"]
WORK_PATH            = BASE_PATH   + bs_conf_dic["WORK_PATH"]
PREPROCESS_PATH      = bs_conf_dic["PREPROCESS_PATH"]
DATABRICKS_PREFIX    = bs_conf_dic["DATABRICKS_PREFIX"]
AIBEACON_PATH        = bs_conf_dic["AIBEACON_PATH"]

# COMMAND ----------
# 案件レベル設定ファイルの読み込み
pj_conf_dic = get_yaml_dict(DATABRICKS_PREFIX + WORK_PATH + PROJECT_NAME + '/input/project_config.yaml')
print(pj_conf_dic)

# 案件レベル設定を取得する
ANALYSIS_OBJECT:Literal['AI_BEACON', 'GPS_DATA'] = pj_conf_dic["analysis_object"]
NETWORK_LIST_FILE = pj_conf_dic['network_list_file']


# COMMAND ----------

# 簡易デバッグ用
# databricksのウィジット・ジョブからの設定内容
print(f'SETTING_TASK_NAME: {SETTING_TASK_NAME}')
print(f'PROJECT_NAME:      {PROJECT_NAME}')
print(f'SPECIFIED_PERIOD:  {SPECIFIED_PERIOD}')

# 基本設定ファイルの設定内容
print(f'WORK_PATH:         {WORK_PATH}')
print(f'PREPROCESS_PATH:   {PREPROCESS_PATH}')
print(f'DATABRICKS_PREFIX: {DATABRICKS_PREFIX}')
print(f'AIBEACON_PATH:     {AIBEACON_PATH}')

# 案件レベル設定ファイルの設定内容
print(f'ANALYSIS_OBJECT:   {ANALYSIS_OBJECT}')
print(f'NETWORK_LIST_FILE: {NETWORK_LIST_FILE}')


# COMMAND ----------
need_col = ['folder_name', 'floor_name', 'user_id', 'place_id', 'network_id', 'unit_id', 'rssi_fx']


# 設定ファイルから生データを取得する
nwm_conf = read_csv_file(WORK_PATH + PROJECT_NAME + "/input/" + NETWORK_LIST_FILE)
nwm_conf = nwm_conf.select(need_col)

# データフレームの永続化
nwm_conf.persist(StorageLevel.MEMORY_ONLY)
nwm_conf.count()
nwm_conf.display()

# COMMAND ----------

# 日付を設定する
dates = [datetime.datetime.strptime(sp_date, '%Y%m%d').strftime('%Y-%m-%d') for sp_date in SPECIFIED_PERIOD]
print(dates)

# COMMAND ----------
nwm_list = sorted(row['unit_id'] for row in nwm_conf.select('unit_id').dropDuplicates().collect())
nwm_list = list(map(lambda x: str(x), nwm_list))

# 生データと設定ファイルとを突合する
df_raw_data = spark.table(AIBEACON_PATH)\
                    .filter(col('date').isin(dates))\
                    .filter(col('randomized') == '1')\
                    .select(['date', 'datetime', 'unit_id', 'aibeaconid', 'rssi'])\
                    .filter(col('unit_id').isin(nwm_list))\
                    .join(nwm_conf, on='unit_id', how='inner')\
                    .filter(col('rssi') <= col('rssi_fx'))\
                    .select(['date', 'datetime', 'folder_name', 'user_id', 'place_id', 'floor_name', 'network_id', 'unit_id', 'aibeaconid'])
                    # 指定の日付 かつ
                    # ランダムMACアドレス かつ
                    # 指定の列のみを抽出
                    # unit_idが一致している かつ
                    # rssi <= rssi_fx かつ
                    # 指定の列のみを抽出
        
    
# 各列の型の明示
df_raw_data = df_raw_data\
                    .withColumn('date',        col('date').cast(       types.StringType()))\
                    .withColumn('datetime',    col('datetime').cast(   types.StringType()))\
                    .withColumn('folder_name', col('folder_name').cast(types.StringType()))\
                    .withColumn('user_id',     col('user_id').cast(    types.StringType()))\
                    .withColumn('place_id',    col('place_id').cast(   types.StringType()))\
                    .withColumn('floor_name',  col('floor_name').cast( types.StringType()))\
                    .withColumn('network_id',  col('network_id').cast( types.StringType()))\
                    .withColumn('unit_id',     col('unit_id').cast(    types.StringType()))\
                    .withColumn('aibeaconid',  col('aibeaconid').cast( types.StringType()))

df_raw_data.display()

# COMMAND ----------
# メモ：
# 1分ごとの集計データを単純に足し合わせて、1日分のデータと比較しても一致しない
# 同様に、1分ごとの集計データと1時間ごとの集計データを比較したとしても一致しない
# 1時間ごとの集計データと1日分の集計データの比較についても同じである
# これは重複削除(dropDuplicates)の有効な時間区間が異なるためである
# 時間粒度外の重複するユニークIDについては重複削除しない
# そのため、1時間ごとでは重複するユニークIDが1分ごとでは重複しないパターンが発生する
# この差異については、バグではなく仕様であることに注意すること

# 日毎に前処理データを用意する
for date in tqdm(dates):
    print(date)
    df_tmp = df_raw_data.filter(col('date') == date)
        
    # アップローダの設定
    use_upload = original_UL(DATABRICKS_PREFIX, WORK_PATH + PROJECT_NAME, PREPROCESS_PATH, date)
    
    
    # 少しでも高速化するためにここでキャッシュを保存する
    df_tmp.persist(StorageLevel.MEMORY_ONLY)
    df_tmp.count()
    
    # AI Beacon用の前処理
    create_ai_beacon_data(use_upload, df_tmp)
    
    # 占有リソースの解放
    df_tmp.unpersist()


# COMMAND ----------
# 占有リソースの解放
nwm_conf.unpersist()
df_raw_data.unpersist()