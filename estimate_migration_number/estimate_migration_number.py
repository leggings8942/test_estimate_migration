# Databricks notebook source
# 推定回遊人数（単位：人数）の計算

# E=MC^2を施す
# Covid-19における「人流から10日後RNA正規化値を予測する」処理と同様に
# 「O→D」因果量を「Dの時系列データにおける各時刻の値の2乗」で割る。
# →1日における「O→D」の人数を得る

# テストプレイ
# シュレディンガー方程式を時系列データに落とし込むとdeltaDの縮約？？？

# COMMAND ----------

# test_2_1_b
# 来訪者数量が一致できるように調整する
# parquetファイルで出力している
# devで言うところのココにある
# /Workspace/Users/hirako@adintedmp.onmicrosoft.com/custom_order/nomura_fudosan/dev/test_2_1_b/parquet/test_estimate_migration_number_hourly_parquet_日次出力

# COMMAND ----------
import yaml
from typing import Literal
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

from UseCases.calc_estimate_migration_number import calc_estimate_migration_number
from _injector import original_AIB_EM

# COMMAND ----------
builder = SparkSession.builder\
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")\
            .config("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")\
            .config("spark.sql.shuffle.partitions", 100)\
            .config("spark.sql.dynamicPartitionPruning.enabled", True)\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .config("spark.databricks.delta.optimizeWrite.enabled", True)\
            .config("spark.databricks.delta.autoCompact.enabled", True)
            # '_started'と'_committed_'で始まるファイルを書き込まないように設定
            # '_SUCCESS'で始まるファイルを書き込まないように設定
            # パーティション数を調整する
            # 動的パーティションプルーニングの有効化
            # Delta Lake固有のSQL構文や解析機能を拡張モジュールとして有効化
            # SparkカタログをDeltaLakeカタログへ変更
            # 書き込み時にデータシャッフルを行い、大きなファイルを生成する機能の有効化
            # 書き込み後に小さなファイルを自動で統合する機能の有効化


spark = configure_spark_with_delta_pip(builder).getOrCreate()


# COMMAND ----------
def get_yaml_dict(path:str) -> dict:
    try:
        with open(path, 'r') as yml:
            bs_conf_dic = yaml.safe_load(yml)
    except Exception as e:
        print("エラーが発生しました::", e)
        print(f"yamlファイルの読み込みに失敗しました。")
        raise FileNotFoundError("file not found.")
    
    return bs_conf_dic

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
# ウェジットからの設定の読み込み
PROJECT_NAME      = get_widgets_var('PROJECT_NAME')
SPECIFIED_DATE    = get_widgets_var('SPECIFIED_DATE')
PART_ID           = get_widgets_var('PART_ID')

# COMMAND ----------
# 基本設定ファイルの読み込み
bs_conf_dic = get_yaml_dict('../config/basic_config.yaml')
print(bs_conf_dic)

# 基本設定を取得する
BASE_PATH         = bs_conf_dic['BASE_PATH']
WORK_PATH         = BASE_PATH   + bs_conf_dic['WORK_PATH']
INTERMEDIATE_PATH = bs_conf_dic['INTERMEDIATE_PATH']
OUTPUT_PATH       = bs_conf_dic['OUTPUT_PATH']
DATABRICKS_PREFIX = bs_conf_dic['DATABRICKS_PREFIX']


# COMMAND ----------
# 案件レベル設定ファイルの読み込み
pj_conf_dic = get_yaml_dict(DATABRICKS_PREFIX + WORK_PATH + PROJECT_NAME + '/input/project_config.yaml')
print(pj_conf_dic)

# 案件レベル設定を取得する
ANALYSIS_OBJECT:Literal['AI_BEACON', 'GPS_DATA'] = pj_conf_dic["analysis_object"]
NETWORK_LIST_FILE = pj_conf_dic['network_list_file']
ENABLE_GROUP_MODE:Literal[True, False] = pj_conf_dic['enable_group_mode']
ENABLE_OLD_PATH_POLICY:Literal[True, False] = pj_conf_dic['enable_old_path_policy']

# COMMAND ----------

# 簡易デバッグ用
# databricksのウィジット・ジョブからの設定内容
print(f'PROJECT_NAME:      {PROJECT_NAME}')
print(f'SPECIFIED_DATE:    {SPECIFIED_DATE}')
print(f'PART_ID:           {PART_ID}')

# 基本設定ファイルの設定内容
print(f'WORK_PATH:         {WORK_PATH}')
print(f'INTERMEDIATE_PATH: {INTERMEDIATE_PATH}')
print(f'OUTPUT_PATH:       {OUTPUT_PATH}')
print(f'DATABRICKS_PREFIX: {DATABRICKS_PREFIX}')

# 案件レベル設定ファイルの設定内容
print(f'ANALYSIS_OBJECT:        {ANALYSIS_OBJECT}')
print(f'NETWORK_LIST_FILE:      {NETWORK_LIST_FILE}')
print(f'ENABLE_GROUP_MODE:      {ENABLE_GROUP_MODE}')
print(f'ENABLE_OLD_PATH_POLICY: {ENABLE_OLD_PATH_POLICY}')


# COMMAND ----------

# 日付文字列の変換
SPECIFIED_DATE = datetime.strptime(SPECIFIED_DATE, '%Y%m%d').strftime('%Y-%m-%d')

# FILE IOモデルの選択
if ANALYSIS_OBJECT == 'AI_BEACON':
    # AI Beacon用のFILE IOモデル
    use_model = original_AIB_EM(
                    DATABRICKS_PREFIX,
                    WORK_PATH + PROJECT_NAME,
                    INTERMEDIATE_PATH,
                    OUTPUT_PATH,
                    PART_ID,
                    SPECIFIED_DATE,
                    ENABLE_GROUP_MODE,
                    ENABLE_OLD_PATH_POLICY,
                )
else:
    # GPS Data用のFILE IOモデル
    use_model = original_GPS_EM(
                    DATABRICKS_PREFIX,
                    WORK_PATH + PROJECT_NAME,
                    INTERMEDIATE_PATH,
                    OUTPUT_PATH,
                    PART_ID,
                    SPECIFIED_DATE,
                    ENABLE_GROUP_MODE,
                    ENABLE_OLD_PATH_POLICY,
                )


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

# ネットワークリストファイルの読み込み
nwm_conf = use_model.read_csv_file('input/' + NETWORK_LIST_FILE)

if ANALYSIS_OBJECT == "AI_BEACON":
    # 対象案件におけるflid_listのリストを取得する
    flid_list = sorted(row['floor_name'] for row in nwm_conf.filter(col('place_id') == PART_ID).select(col('floor_name')).drop_duplicates().collect())
    flid_list = list(map(lambda x: str(x), flid_list))
    

    param_dict_nwm = {
        'date'                            : SPECIFIED_DATE,
        'part_id'                         : PART_ID,
        'section_id_list'                 : flid_list,
        'daily_estimate_migration_number' : pj_conf_dic['daily_estimate_migration_number'],
        'hourly_estimate_migration_number': pj_conf_dic['hourly_estimate_migration_number'],
    }
    
    # estimate_migration_number処理
    calc_estimate_migration_number(use_model, param_dict_nwm)

else:
    # 将来的に実装予定
    unid_list = ['undefined']
    
    param_dict_nwm = {
        'date'                            : SPECIFIED_DATE,
        'part_id_list'                    : PART_ID,
        'section_id_dict'                 : unid_list,
        'daily_estimate_migration_number' : pj_conf_dic['daily_estimate_migration_number'],
        'hourly_estimate_migration_number': pj_conf_dic['hourly_estimate_migration_number'],
    }
    
    # estimate_migration_number処理
    calc_estimate_migration_number(use_model, param_dict_nwm)


# COMMAND ----------
# メモ：
# なぜdel文を利用せずに直接__del__()関数を呼び出しているか言えば、エラー処理ができなくなるためである
# 実際に試したところ、del文の実行中に投げられたエラーは握りつぶされていることがわかった
# databricks側にエラーとして認識されるため、直接__del__()関数を呼び出すことにした

# このオブジェクト内部で非同期処理を行っているため、明示的な削除により非同期処理の終了を保証する
use_model.__del__()