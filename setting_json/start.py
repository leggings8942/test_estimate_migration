# Databricks notebook source
# MAGIC %md
# MAGIC databricks上での動作用のjson scriptを生成する

# COMMAND ----------
import json
import yaml
from typing import Literal
import itertools
import datetime, pytz
from dateutil.relativedelta import relativedelta
import pandas as pd

import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

from easy_test import easy_test as ea
from easy_test import test_project_config

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

# COMMAND ----------
# databricksのウィジットからの設定の読み込み
PROJECT_NAME         = get_widgets_var("PROJECT_NAME")                                # プロジェクト名
RUN_MODE:Literal['REGULAR_EXECUTION', 'SPOT_EXECUTION'] = get_widgets_var("RUN_MODE") # 実行モード
SPECIFIED_START_DATE = get_widgets_var("SPECIFIED_START_DATE")                        # 指定した開始日
SPECIFIED_END_DATE   = get_widgets_var("SPECIFIED_END_DATE")                          # 指定した終了日

# COMMAND ----------
# メモ：
# 本来ならば基本設定ファイルの設定内容の確認も行うべきである
# しかしこのファイルの設定内容は全てのプロジェクトに共通するものであり、滅多に編集することはない
# 個々の案件ごとに実行ごとに確認処理を行うのは非効率である
# そのため、この設定ファイルに対する確認処理は行わない事とする

# 基本設定ファイルの読み込み
bs_conf_dic = get_yaml_dict('../config/basic_config.yaml')
print(bs_conf_dic)

# 基本設定を取得する
BASE_PATH         = bs_conf_dic["BASE_PATH"]
WORK_PATH         = BASE_PATH + bs_conf_dic["WORK_PATH"]
DATABRICKS_PREFIX = bs_conf_dic["DATABRICKS_PREFIX"]

# COMMAND ----------
# メモ：
# 案件レベル設定ファイルの設定内容に記述ミスがないかをsetting_json実行段階で全て確認する
# もしも記述ミスが存在するのであれば、その場でエラーを発生させて後続の処理を行わない事とする

# 案件レベル設定ファイルの読み込み
pj_conf_dic = get_yaml_dict(DATABRICKS_PREFIX + WORK_PATH + PROJECT_NAME + '/input/project_config.yaml')
print(pj_conf_dic)

ANALYSIS_OBJECT:Literal['AI_BEACON', 'GPS_DATA'] = pj_conf_dic["analysis_object"]
NETWORK_LIST_FILE:str                            = pj_conf_dic['network_list_file']
USE_MODEL_TYPE:str                               = pj_conf_dic['use_model_type']
ENABLE_GROUP_MODE:Literal[True, False]           = pj_conf_dic['enable_group_mode']
ENABLE_AIB_ROW_DATA:Literal[True, False]         = pj_conf_dic['enable_aib_row_data']
ENABLE_SPECIFY_TIME:Literal[True, False]         = pj_conf_dic['enable_specify_time']
ENABLE_OLD_PATH_POLICY:Literal[True, False]      = pj_conf_dic['enable_old_path_policy']
DAILY_START_TIME:str                             = pj_conf_dic['daily_start_time']
DAILY_END_TIME:str                               = pj_conf_dic['daily_end_time']
DAILY_TIME_INTERVAL:str                          = pj_conf_dic['daily_time_interval']
DAILY_POPULATION_AREA:str                        = pj_conf_dic['daily_population_area']
DAILY_ESTIMATE_MIGRATION_NUMBER:list[str|int]    = pj_conf_dic['daily_estimate_migration_number']
DAILY_VISITOR_NUMBER:str                         = pj_conf_dic['daily_visitor_number']
HOURLY_START_TIME:str                            = pj_conf_dic['hourly_start_time']
HOURLY_END_TIME:str                              = pj_conf_dic['hourly_end_time']
HOURLY_TIME_INTERVAL:str                         = pj_conf_dic['hourly_time_interval']
HOURLY_POPULATION_AREA:str                       = pj_conf_dic['hourly_population_area']
HOURLY_ESTIMATE_MIGRATION_NUMBER:list[str|int]   = pj_conf_dic['hourly_estimate_migration_number']
HOURLY_VISITOR_NUMBER:str                        = pj_conf_dic['hourly_visitor_number']


# COMMAND ----------

# 簡易デバッグ用
# databricksのウィジット・ジョブからの設定内容
print(f'PROJECT_NAME:                     {PROJECT_NAME}')
print(f'RUN_MODE:                         {RUN_MODE}')
print(f'SPECIFIED_START_DATE:             {SPECIFIED_START_DATE}')
print(f'SPECIFIED_END_DATE:               {SPECIFIED_END_DATE}')

# 基本設定ファイルの設定内容
print(f'WORK_PATH:                        {WORK_PATH}')
print(f'DATABRICKS_PREFIX:                {DATABRICKS_PREFIX}')

# 案件レベル設定ファイルの設定内容
print(f'ANALYSIS_OBJECT:                  {ANALYSIS_OBJECT}')
print(f'NETWORK_LIST_FILE:                {NETWORK_LIST_FILE}')
print(f'USE_MODEL_TYPE:                   {USE_MODEL_TYPE}')
print(f'ENABLE_GROUP_MODE:                {ENABLE_GROUP_MODE}')
print(f'ENABLE_AIB_ROW_DATA:              {ENABLE_AIB_ROW_DATA}')
print(f'ENABLE_SPECIFY_TIME:              {ENABLE_SPECIFY_TIME}')
print(f'ENABLE_OLD_PATH_POLICY:           {ENABLE_OLD_PATH_POLICY}')
print(f'DAILY_START_TIME:                 {DAILY_START_TIME}')
print(f'DAILY_END_TIME:                   {DAILY_END_TIME}')
print(f'DAILY_TIME_INTERVAL:              {DAILY_TIME_INTERVAL}')
print(f'DAILY_POPULATION_AREA:            {DAILY_POPULATION_AREA}')
print(f'DAILY_ESTIMATE_MIGRATION_NUMBER:  {DAILY_ESTIMATE_MIGRATION_NUMBER}')
print(f'DAILY_VISITOR_NUMBER:             {DAILY_VISITOR_NUMBER}')
print(f'HOURLY_START_TIME:                {HOURLY_START_TIME}')
print(f'HOURLY_END_TIME:                  {HOURLY_END_TIME}')
print(f'HOURLY_TIME_INTERVAL:             {HOURLY_TIME_INTERVAL}')
print(f'HOURLY_POPULATION_AREA:           {HOURLY_POPULATION_AREA}')
print(f'HOURLY_ESTIMATE_MIGRATION_NUMBER: {HOURLY_ESTIMATE_MIGRATION_NUMBER}')
print(f'HOURLY_VISITOR_NUMBER:            {HOURLY_VISITOR_NUMBER}')


# COMMAND ----------

# 案件レベル設定ファイルに対する簡易テスト
test_data = ea(
                ANALYSIS_OBJECT=ANALYSIS_OBJECT,
                NETWORK_LIST_FILE=NETWORK_LIST_FILE,
                USE_MODEL_TYPE=USE_MODEL_TYPE,
                ENABLE_GROUP_MODE=ENABLE_GROUP_MODE,
                ENABLE_AIB_ROW_DATA=ENABLE_AIB_ROW_DATA,
                ENABLE_SPECIFY_TIME=ENABLE_SPECIFY_TIME,
                ENABLE_OLD_PATH_POLICY=ENABLE_OLD_PATH_POLICY,
                DAILY_START_TIME=DAILY_START_TIME,
                DAILY_END_TIME=DAILY_END_TIME,
                DAILY_TIME_INTERVAL=DAILY_TIME_INTERVAL,
                DAILY_POPULATION_AREA=DAILY_POPULATION_AREA,
                DAILY_ESTIMATE_MIGRATION_NUMBER=DAILY_ESTIMATE_MIGRATION_NUMBER,
                DAILY_VISITOR_NUMBER=DAILY_VISITOR_NUMBER,
                HOURLY_START_TIME=HOURLY_START_TIME,
                HOURLY_END_TIME=HOURLY_END_TIME,
                HOURLY_TIME_INTERVAL=HOURLY_TIME_INTERVAL,
                HOURLY_POPULATION_AREA=HOURLY_POPULATION_AREA,
                HOURLY_ESTIMATE_MIGRATION_NUMBER=HOURLY_ESTIMATE_MIGRATION_NUMBER,
                HOURLY_VISITOR_NUMBER=HOURLY_VISITOR_NUMBER,
            )
test_project_config(ea=test_data)

# COMMAND ----------
# ネットワークリストファイルの読み込み
NETWORK_FILE_PATH = WORK_PATH + PROJECT_NAME + "/input/" + NETWORK_LIST_FILE
df_network = spark.read\
                    .option('inferSchema', 'False')\
                    .option('header', 'True')\
                    .csv(NETWORK_FILE_PATH)


# COMMAND ----------
if RUN_MODE == "REGULAR_EXECUTION":
    dt     = datetime.datetime.now(tz=pytz.timezone('Asia/Tokyo'))
    dt_aib = dt - relativedelta(days=1) + datetime.timedelta(hours=9)
    dt_gps = dt - relativedelta(days=8) + datetime.timedelta(hours=9)
    
    if ANALYSIS_OBJECT == 'AI_BEACON':
        day_list = [dt_aib.strftime('%Y%m%d')]
    else:
        day_list = [dt_gps.strftime('%Y%m%d')]
else:
    start_date = SPECIFIED_START_DATE
    end_date   = SPECIFIED_END_DATE
    day_list   = [d.strftime("%Y%m%d") for d in pd.date_range(start_date, end_date)]

# COMMAND ----------
# 各種IDの対応一覧
# ・AI Beacon
#  |-- part_id:       place_id
#  |-- section_id:    floor_name
#  |-- subsection_id: network_id
#  |-- unique_id:     unit_id
# 
# ・GPS Data
#  |-- part_id:       user_id
#  |-- section_id:    undefined
#  |-- subsection_id: network_id
#  |-- unique_id:     place_id


# AI Beaconの場合にはplace_idがpart_idとなる
# GPS Data の場合にはuser_id がpart_idとなる
if ANALYSIS_OBJECT == 'AI_BEACON':
    part_id_list = sorted(row['place_id'] for row in df_network.select(col('place_id')).drop_duplicates().collect())
else:
    part_id_list = sorted(row['user_id']  for row in df_network.select(col('user_id')).drop_duplicates().collect())



# 旧バージョンで利用されていたパスポリシーが有効なら
if ENABLE_OLD_PATH_POLICY:
    if not len(part_id_list) == 1:
        print(f"旧バージョンで利用されていたパスポリシーが有効な場合、part_idの数は1つでなければなりません。")
        print(f"len(part_id_list) = {len(part_id_list)}")
        raise ValueError("part_idの数が1つではありません。")
    
    if DAILY_POPULATION_AREA == 'SEPARATE_FLOOR':
        print(f"旧バージョンで利用されていたパスポリシーが有効な場合、人気エリアの解析対象は全フロアでなければなりません。")
        print(f"DAILY_POPULATION_AREA = {DAILY_POPULATION_AREA}")
        raise ValueError("DAILY_POPULATION_AREAの設定に間違いがあります")
        
    if DAILY_ESTIMATE_MIGRATION_NUMBER[0] == 'SEPARATE_FLOOR':
        print(f"旧バージョンで利用されていたパスポリシーが有効な場合、推定回遊人数の解析対象は全フロアでなければなりません。")
        print(f"DAILY_ESTIMATE_MIGRATION_NUMBER[0] = {DAILY_ESTIMATE_MIGRATION_NUMBER[0]}")
        raise ValueError("DAILY_ESTIMATE_MIGRATION_NUMBER[0]の設定に間違いがあります")
        
    if DAILY_VISITOR_NUMBER == 'SEPARATE_FLOOR':
        print(f"旧バージョンで利用されていたパスポリシーが有効な場合、推定来訪者数の解析対象は全フロアでなければなりません。")
        print(f"DAILY_VISITOR_NUMBER = {DAILY_VISITOR_NUMBER}")
        raise ValueError("DAILY_VISITOR_NUMBERの設定に間違いがあります")
    
    if HOURLY_POPULATION_AREA == 'SEPARATE_FLOOR':
        print(f"旧バージョンで利用されていたパスポリシーが有効な場合、人気エリアの解析対象は全フロアでなければなりません。")
        print(f"HOURLY_POPULATION_AREA = {HOURLY_POPULATION_AREA}")
        raise ValueError("HOURLY_POPULATION_AREAの設定に間違いがあります")
        
    if HOURLY_ESTIMATE_MIGRATION_NUMBER[0] == 'SEPARATE_FLOOR':
        print(f"旧バージョンで利用されていたパスポリシーが有効な場合、推定回遊人数の解析対象は全フロアでなければなりません。")
        print(f"HOURLY_ESTIMATE_MIGRATION_NUMBER[0] = {HOURLY_ESTIMATE_MIGRATION_NUMBER[0]}")
        raise ValueError("HOURLY_ESTIMATE_MIGRATION_NUMBER[0]の設定に間違いがあります")
        
    if HOURLY_VISITOR_NUMBER == 'SEPARATE_FLOOR':
        print(f"旧バージョンで利用されていたパスポリシーが有効な場合、推定来訪者数の解析対象は全フロアでなければなりません。")
        print(f"HOURLY_VISITOR_NUMBER = {HOURLY_VISITOR_NUMBER}")
        raise ValueError("HOURLY_VISITOR_NUMBERの設定に間違いがあります")


# COMMAND ----------
# 並列実行用JSON
json_list = [
    {
        # ウィジット関連
        "PROJECT_NAME":     PROJECT_NAME,
        "SPECIFIED_DATE":   date,
        "PART_ID":          part_id,
    }
    for date, part_id in itertools.product(day_list, part_id_list)
]
print(json_list[0],json_list[-1])

# preprocess用のJSON
json_period = {
    # ウィジット関連
    "PROJECT_NAME":     PROJECT_NAME,
    "SPECIFIED_PERIOD": day_list,
}

# COMMAND ----------
target_json = json.dumps(json_list,   indent=4)
print(target_json)

period_json = json.dumps(json_period, indent=4)
print(period_json)

# COMMAND ----------
# メモ：
# 案件の規模によっては、Databricksのタスク上限に引っかかってしまうことがある
# その場合には以下のようなエラーが発生する
# Py4JJavaError: An error occurred while calling o480.setJson.: com.databricks.common.client.DatabricksServiceHttpClientException: INVALID_PARAMETER_VALUE: The task value is too large
# このようなエラーの回避のためには、複数のJOBに分けて再実行するしか方法はない
# そのため、リトライロジックのようなコードはそもそも書かない事とする
dbutils.jobs.taskValues.set('target_json',   target_json)
dbutils.jobs.taskValues.set('target_period', period_json)