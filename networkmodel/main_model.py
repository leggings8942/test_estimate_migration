# Databricks notebook source
# MAGIC %md
# MAGIC #### ver2.1からver2.2への変更内容
# MAGIC - 対象案件に関係なく、日別と1時間別を出力しておく
# MAGIC - 直行化撹乱項の出力を廃止
# MAGIC - 直交化インパルス応答関数（回遊2階層）の出力を廃止
# MAGIC - 直行化インパルス応答関数の１ショックの基準を１偏差から１単位へと変更
# MAGIC - データソースを以下の２つのテーブルから選択できるように変更
# MAGIC     - adinte.aibeacon_wifi_log
# MAGIC     - adinte_analyzed_data.gps_contact
# MAGIC - 出力パスと出力ファイルの変更
# MAGIC     - 日別・時間別の出力ファイルを生成するにあたり、できる限り出力ファイルを<br>
# MAGIC       まとめるように変更した。
# MAGIC     - 人気エリアの出力をsilerレイヤへと変更
# MAGIC     - 新規日別の出力ファイル<br>
# MAGIC >       silver/{folder_name}/intermediate/daily/{%Y-%m-%d}_causality.csv
# MAGIC >       silver/{folder_name}/intermediate/daily/{%Y-%m-%d}_centrality.csv
# MAGIC >       silver/{folder_name}/intermediate/daily/{%Y-%m-%d}_migration_3.csv
# MAGIC >       silver/{folder_name}/intermediate/daily/{%Y-%m-%d}_migration_4.csv
# MAGIC >       silver/{folder_name}/output/daily/{%Y-%m-%d}_人気エリア.csv
# MAGIC     - 新規時間別の出力ファイル<br>
# MAGIC >       silver/{folder_name}/intermediate/hourly/{%Y-%m-%d}_causality.csv
# MAGIC >       silver/{folder_name}/intermediate/hourly/{%Y-%m-%d}_centrality.csv
# MAGIC >       silver/{folder_name}/intermediate/hourly/{%Y-%m-%d}_migration_3.csv
# MAGIC >       silver/{folder_name}/intermediate/hourly/{%Y-%m-%d}_migration_4.csv
# MAGIC >       silver/{folder_name}/output/hourly/{%Y-%m-%d}_人気エリア.csv

# COMMAND ----------
%pip install networkx==3.3

# COMMAND ----------
import yaml
from typing import Literal
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from UseCases.Entities.data_reconstruction import get_unique_to_section_dict, get_subsection_to_section_dict
from UseCases.networkmodel import networkmodel
from UseCases._interface import UseInterface
from _injector import public_VAR, original_VAR, original_SVAR
from _injector import original_AIB_UL
from _injector import original_AIB_DL


# COMMAND ----------
builder = SparkSession.builder\
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")\
            .config("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")\
            .config("spark.sql.adaptive.enabled", True)\
            .config("spark.sql.dynamicPartitionPruning.enabled", True)\
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1MB")\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .config("spark.databricks.delta.optimizeWrite.enabled", True)\
            .config("spark.databricks.delta.autoCompact.enabled", True)
            # '_started'と'_committed_'で始まるファイルを書き込まないように設定
            # '_SUCCESS'で始まるファイルを書き込まないように設定
            # AQE(Adaptive Query Execution)の有効化
            # 動的パーティションプルーニングの有効化
            # シャッフル後の1パーティションあたりの最小サイズを指定
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
PREPROCESS_PATH   = bs_conf_dic['PREPROCESS_PATH']
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
USE_MODEL_TYPE    = pj_conf_dic['use_model_type']
ENABLE_GROUP_MODE:Literal[True, False]   = pj_conf_dic['enable_group_mode']
ENABLE_SPECIFY_TIME:Literal[True, False] = pj_conf_dic['enable_specify_time']
ENABLE_OLD_PATH_POLICY:Literal[True, False]   = pj_conf_dic['enable_old_path_policy']

# COMMAND ----------

# 簡易デバッグ用
# databricksのウィジット・ジョブからの設定内容
print(f'PROJECT_NAME:      {PROJECT_NAME}')
print(f'SPECIFIED_DATE:    {SPECIFIED_DATE}')
print(f'PART_ID:           {PART_ID}')

# 基本設定ファイルの設定内容
print(f'WORK_PATH:         {WORK_PATH}')
print(f'PREPROCESS_PATH:   {PREPROCESS_PATH}')
print(f'INTERMEDIATE_PATH: {INTERMEDIATE_PATH}')
print(f'OUTPUT_PATH:       {OUTPUT_PATH}')
print(f'DATABRICKS_PREFIX: {DATABRICKS_PREFIX}')

# 案件レベル設定ファイルの設定内容
print(f'ANALYSIS_OBJECT:        {ANALYSIS_OBJECT}')
print(f'NETWORK_LIST_FILE:      {NETWORK_LIST_FILE}')
print(f'USE_MODEL_TYPE:         {USE_MODEL_TYPE}')
print(f'ENABLE_GROUP_MODE:      {ENABLE_GROUP_MODE}')
print(f'ENABLE_SPECIFY_TIME:    {ENABLE_SPECIFY_TIME}')
print(f'ENABLE_OLD_PATH_POLICY: {ENABLE_OLD_PATH_POLICY}')


# COMMAND ----------

# 日付文字列の変換
SPECIFIED_DATE = datetime.strptime(SPECIFIED_DATE, '%Y%m%d').strftime('%Y-%m-%d')

# 解析モデルの選択
if   USE_MODEL_TYPE.lower() in ['external', 'external library', 'external_library', 'el']:
    use_model = public_VAR()                                                                # 使用する時系列解析モデル
elif USE_MODEL_TYPE.lower() in ['sparse',   'sparse model',     'sparse_model',     'sm']:
    use_model = original_SVAR()                                                             # 使用する時系列解析モデル
else:
    use_model = original_VAR()                                                              # 使用する時系列解析モデル

# ダウンローダの選択
if ANALYSIS_OBJECT == "AI_BEACON":
    # AI Beacon用のダウンローダ
    use_download = original_AIB_DL(
                        DATABRICKS_PREFIX,
                        WORK_PATH + PROJECT_NAME,
                        PREPROCESS_PATH,
                        PART_ID,
                        SPECIFIED_DATE
                    )
else:
    # GPS Data用のダウンローダ
    use_download = original_GPS_DL(
                        DATABRICKS_PREFIX,
                        WORK_PATH + PROJECT_NAME,
                        PREPROCESS_PATH,
                        PART_ID,
                        SPECIFIED_DATE
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

nwm_conf = use_download.read_csv_file('input/' + NETWORK_LIST_FILE)
if ANALYSIS_OBJECT == "AI_BEACON":
    nwm_tmp   = nwm_conf.astype({'place_id': str})
    nwm_tmp   = nwm_tmp[nwm_tmp['place_id'] == PART_ID]
    
    # 特定のプレイスIDに対応するユニットIDをリストに登録
    utid_list = sorted(nwm_tmp['unit_id'].drop_duplicates().to_list())
    utid_list = list(map(lambda x: str(x), utid_list))
    
    # floor_name・network_id・unit_idの対応辞書を取得する
    fl2ut_dict    = {}
    fl2nk2ut_dict = {}
    
    # 特定のプレイスIDに対応するフロアIDを辞書に登録
    flid_list = sorted(nwm_tmp['floor_name'].drop_duplicates().to_list())
    flid_list = list(map(lambda x: str(x), flid_list))
    for floor_name in flid_list:
        nwm_tmp2 = nwm_tmp.astype({'floor_name': str})
        nwm_tmp2 = nwm_tmp2[nwm_tmp2['floor_name'] == floor_name]
    
        # 特定のフロアIDに対応するユニットIDを辞書に登録
        fl2ut_list = sorted(nwm_tmp2['unit_id'].drop_duplicates().to_list())
        fl2ut_list = list(map(lambda x: str(x), fl2ut_list))
        fl2ut_dict[floor_name] = fl2ut_list
            
        # 特定のフロアIDに対応するネットワークIDを辞書に登録
        nkid_list = sorted(nwm_tmp2['network_id'].drop_duplicates().to_list())
        nkid_list = list(map(lambda x: str(x), nkid_list))
        fl2nk2ut_dict[floor_name] = {}
        for network_id in nkid_list:
            nwm_tmp3 = nwm_tmp2.astype({'network_id': str})
            nwm_tmp3 = nwm_tmp3[nwm_tmp3['network_id'] == network_id]
            
            # 特定のネットワークIDに対応するユニットIDを辞書に登録
            fl2nk2ut_list = sorted(nwm_tmp3['unit_id'].drop_duplicates().to_list())
            fl2nk2ut_list = list(map(lambda x: str(x), fl2nk2ut_list))
            fl2nk2ut_dict[floor_name][network_id] = fl2nk2ut_list
    
    
    # 特定のプレイスIDに対応するユーザーIDをリストに登録
    urid_list = sorted(nwm_tmp['user_id'].drop_duplicates().to_list())
    urid_list = list(map(lambda x: str(x), urid_list))
    if not len(urid_list) == 1:
        # ユーザーIDが複数存在するなら
        print(f'len(user_id_list) = {len(urid_list)}')
        print('一つのplace_idに対して複数のuser_idが割り当てられています')
        raise ValueError('Multiple user_id are assigned to one place_id')
    
    # ユニークID単位のセクションID辞書を取得する
    ue_sn_id_dict = get_unique_to_section_dict(fl2ut_dict)
    # グループモードが有効な場合
    if ENABLE_GROUP_MODE:
        # サブセクション単位のセクションID辞書を取得する
        ue_sn_id_dict = get_subsection_to_section_dict(fl2nk2ut_dict)
    ue_sn_id_dict = {key:value[0] for key, value in ue_sn_id_dict.items()}
    
    
    # AI Beacon用のアップローダ
    use_upload = original_AIB_UL(
                        DATABRICKS_PREFIX,
                        WORK_PATH + PROJECT_NAME,
                        INTERMEDIATE_PATH,
                        OUTPUT_PATH,
                        PART_ID,
                        SPECIFIED_DATE,
                        urid_list[0],
                        ue_sn_id_dict,
                        ENABLE_GROUP_MODE,
                        ENABLE_OLD_PATH_POLICY,
                    )
    
    # 選択された解析モデル・アップローダ・ダウンローダ
    spec_comb = UseInterface(use_model, use_upload, use_download)
    
    
    # print(utid_list)
    param_dict_nwm = {
        'date'                            : SPECIFIED_DATE,
        'part_id'                         : PART_ID,
        'part_to_unique_list'             : utid_list,
        'section_to_unique_dict'          : fl2ut_dict,
        'subsection_to_unique_dict'       : fl2nk2ut_dict,
        'folder_name'                     : PROJECT_NAME,
        'daily_start_time'                : pj_conf_dic['daily_start_time'],
        'daily_end_time'                  : pj_conf_dic['daily_end_time'],
        'daily_time_interval'             : pj_conf_dic['daily_time_interval'],
        'daily_population_area'           : pj_conf_dic['daily_population_area'],
        'daily_estimate_migration_number' : pj_conf_dic['daily_estimate_migration_number'],
        'hourly_start_time'               : pj_conf_dic['hourly_start_time'],
        'hourly_end_time'                 : pj_conf_dic['hourly_end_time'],
        'hourly_time_interval'            : pj_conf_dic['hourly_time_interval'],
        'hourly_population_area'          : pj_conf_dic['hourly_population_area'],
        'hourly_estimate_migration_number': pj_conf_dic['hourly_estimate_migration_number'],
        'GROUP_MODE_FLAG'                 : ENABLE_GROUP_MODE,
        'SPECIFY_TIME_FLAG'               : ENABLE_SPECIFY_TIME,
    }
    
    # networkmodel処理
    networkmodel(spec_comb, param_dict_nwm)
else:
    # =====================================================================
    # Deprecated: 将来的には削除の予定
    # =====================================================================
    if pj_conf_dic['daily_population_area'] != 'ALL_FLOOR':
        raise ValueError('Not supported for processing SEPARATE FLOOR.')
    if pj_conf_dic['daily_estimate_migration_number'][0] != 'ALL_FLOOR':
        raise ValueError('Not supported for processing SEPARATE FLOOR.')
    if pj_conf_dic['hourly_population_area'] != 'ALL_FLOOR':
        raise ValueError('Not supported for processing SEPARATE FLOOR.')
    if pj_conf_dic['hourly_estimate_migration_number'][0] != 'ALL_FLOOR':
        raise ValueError('Not supported for processing SEPARATE FLOOR.')
    
    
    
    nwm_tmp   = nwm_conf.astype({'user_id': str})
    nwm_tmp   = nwm_tmp[nwm_tmp['user_id'] == PART_ID]
    
    # 特定のユーザーIDに対応するプレイスIDをリストに登録
    peid_list = sorted(nwm_tmp['place_id'].drop_duplicates().to_list())
    peid_list = list(map(lambda x: str(x), peid_list))
    
    # network_id・place_idの対応辞書を取得する
    # GPS Dataにおけるsection_idは未定義であるため、
    un2pe_dict    = {}
    un2nk2pe_dict = {}
    
    # 特定のユーザーIDに対応するプレイスIDを辞書に登録
    un2pe_dict['undefined'] = peid_list
            
    # 特定のユーザーIDに対応するネットワークIDを辞書に登録
    nkid_list = sorted(nwm_tmp['network_id'].drop_duplicates().to_list())
    nkid_list = list(map(lambda x: str(x), nkid_list))
    un2nk2pe_dict['undefined'] = {}
    for network_id in nkid_list:
        nwm_tmp2 = nwm_tmp.astype({'network_id': str})
        nwm_tmp2 = nwm_tmp2[nwm_tmp2['network_id'] == network_id]
        
        # 特定のネットワークIDに対応するプレイスIDを辞書に登録
        un2nk2pe_list = sorted(nwm_tmp2['place_id'].drop_duplicates().to_list())
        un2nk2pe_list = list(map(lambda x: str(x), un2nk2pe_list))
        un2nk2pe_dict['undefined'][network_id] = un2nk2pe_list
    
    
    # print(peid_list)
    param_dict_nwm = {
        'date'                            : SPECIFIED_DATE,
        'part_id'                         : PART_ID,
        'part_to_unique_list'             : peid_list,
        'section_to_unique_dict'          : un2pe_dict,
        'subsection_to_unique_dict'       : un2nk2pe_dict,
        'folder_name'                     : PROJECT_NAME,
        'daily_start_time'                : pj_conf_dic['daily_start_time'],
        'daily_end_time'                  : pj_conf_dic['daily_end_time'],
        'daily_time_interval'             : pj_conf_dic['daily_time_interval'],
        'daily_population_area'           : pj_conf_dic['daily_population_area'],
        'daily_estimate_migration_number' : pj_conf_dic['daily_estimate_migration_number'],
        'hourly_start_time'               : pj_conf_dic['hourly_start_time'],
        'hourly_end_time'                 : pj_conf_dic['hourly_end_time'],
        'hourly_time_interval'            : pj_conf_dic['hourly_time_interval'],
        'hourly_population_area'          : pj_conf_dic['hourly_population_area'],
        'hourly_estimate_migration_number': pj_conf_dic['hourly_estimate_migration_number'],
        'GROUP_MODE_FLAG'                 : ENABLE_GROUP_MODE,
        'SPECIFY_TIME_FLAG'               : ENABLE_SPECIFY_TIME,
    }
    
    # networkmodel処理
    networkmodel(spec_comb, param_dict_nwm)


# COMMAND ----------
# メモ：
# なぜdel文を利用せずに直接__del__()関数を呼び出しているか言えば、エラー処理ができなくなるためである
# 実際に試したところ、del文の実行中に投げられたエラーは握りつぶされていることがわかった
# databricks側にエラーとして認識されるため、直接__del__()関数を呼び出すことにした

# このオブジェクト内部で非同期処理を行っているため、明示的な削除により非同期処理の終了を保証する
use_upload.__del__()