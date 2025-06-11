import numpy as np
import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from typing import Literal
from statsmodels.tsa.api import VAR
from statsmodels.tsa.vector_ar.var_model import VARResultsWrapper

from concurrent.futures import ThreadPoolExecutor, as_completed

import IO_control.export_file as ex
import IO_control.import_file as im
from UseCases._interface import time_series_model
from UseCases._interface import upload_to_file, download_to_file
from my_lib.time_series_model import Vector_Auto_Regressive
from my_lib.time_series_model import Sparse_Vector_Auto_Regressive


# メモ：
# delta lake形式でのデータ管理について
# 型が自動で変換される等の挙動を見せるくせに、型変換がミスであるとカラムが破壊されるバグが存在する
# データの読み込み時にもこのバグは存在し、sparkはエラーを吐かない
# こちらが指定したスキーマと違うデータを読み込んだ場合にはエラーを吐かずに、カラムが破壊された状態で処理を続けようとする
# これらの挙動に対する対策は、扱うファイルのカラム名や型をそもそも間違わないようにするくらいしかない
# 出力元・出力先でsparkを扱う可能性があるファイルについては最新の注意を払い、カラム名・カラム順・カラム型の相違が発生しないようにすること


class public_VAR(time_series_model):
    def __init__(self) -> None:
        super().__init__()
        self.wrap_model:VAR = None
        self.learned_model:VARResultsWrapper = None
    
    def register(self, data:pd.DataFrame) -> bool:
        self.wrap_model = VAR(data.to_numpy())
        return True
    
    def fit(self, lags:int, offset:int=0, solver:str="ols") -> bool:
        self.learned_model = self.wrap_model.fit(maxlags=lags, method=solver)
        return True
    
    def select_order(self, maxlag:int, ic:str="aic", solver:str="ols") -> int:
        res = self.wrap_model.select_order(maxlags=maxlag)
        lags = res.selected_orders[ic]
        self.learned_model = self.wrap_model.fit(maxlags=lags, method=solver)
        raise lags
    
    def irf(self, period:int, orth:bool=False, isStdDevShock:bool=False) -> np.ndarray[np.float64]:
        IRF = self.learned_model.irf(periods=period)
        
        if orth:
            res = IRF.orth_irfs
        else:
            res = IRF.irfs
        
        if (not isStdDevShock) and orth:
            res = res / np.diag(IRF.P)
        
        return res
    
    def get_coef(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        params      = self.learned_model.params
        intercept   = params[0,  :]
        coefficient = params[1:, :]
        return intercept, coefficient


class original_VAR(time_series_model):
    def __init__(self) -> None:
        super().__init__()
        self.wrap_model:Vector_Auto_Regressive = None
    
    def register(self, data:pd.DataFrame) -> bool:
        self.wrap_model = Vector_Auto_Regressive(data.to_numpy())
        return True
    
    def fit(self, lags:int=1, offset:int=0, solver:str="normal equations") -> bool:
        return self.wrap_model.fit(lags=lags, offset=offset, solver=solver)
    
    def select_order(self, maxlag:int=15, ic:str="aic", solver:str="normal equations") -> int:
        return self.wrap_model.select_order(maxlag=maxlag, ic=ic, solver=solver, isVisible=True)
    
    def irf(self, period:int, orth:bool, isStdDevShock:bool) -> np.ndarray[np.float64]:
        return self.wrap_model.irf(period=period, orth=orth, isStdDevShock=isStdDevShock)
    
    def get_coef(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        return self.wrap_model.get_coefficient()


class original_SVAR(time_series_model):
    def __init__(self, norm_α:float=0.1, l1_ratio:float=0.1, tol:float=1e-6, max_iterate:int=3e5) -> None:
        super().__init__()
        self.wrap_model:Sparse_Vector_Auto_Regressive = None
        self.norm_α      = norm_α
        self.l1_ratio    = l1_ratio
        self.tol         = tol
        self.max_iterate = round(max_iterate)
    
    def register(self, data:pd.DataFrame) -> bool:
        self.wrap_model = Sparse_Vector_Auto_Regressive(data.to_numpy(), norm_α=self.norm_α, l1_ratio=self.l1_ratio, tol=self.tol, isStandardization=True, max_iterate=self.max_iterate)
        return True
    
    def fit(self, lags:int=1, offset:int=0, solver:str="FISTA") -> bool:
        return self.wrap_model.fit(lags=lags, offset=offset, solver=solver)
    
    def select_order(self, maxlag:int=15, ic:str="aic", solver:str="FISTA") -> int:
        return self.wrap_model.select_order(maxlag=maxlag, ic=ic, solver=solver, isVisible=True)
    
    def irf(self, period:int, orth:bool, isStdDevShock:bool) -> np.ndarray[np.float64]:
        return self.wrap_model.irf(period=period, orth=orth, isStdDevShock=isStdDevShock)
    
    def get_coef(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        return self.wrap_model.get_coefficient()



# メモ：
# 当初は、AI BeaconとGPS Dataの対応の切り替えをANALYSIS_OBJECT変数によって実現していた
# しかしAI Beacon特有の要求やGPS Data特有の要求を一つのクラスで管理していたために、バグが頻発するようになった
# この反省として、AI BeaconとGPS Dataの対応の切り替えをクラスを分割することで対処することにした
class original_AIB_UL(upload_to_file):
    def __init__(self, DATABRICKS_PREFIX:str, BASE_PATH:str, INTERMEDIATE_PATH:str, OUTPUT_PATH:str, PART_ID:str, DATE:str, USER_ID:str, FLOOR_ID_DICT:dict[str, str], ENABLE_GROUP_MODE:bool, ENABLE_OLD_PATH_POLICY:bool, MAX_WORKERS:int=20) -> None:
        super().__init__()
        self.DATABRICKS_PREFIX      = DATABRICKS_PREFIX
        self.BASE_PATH              = BASE_PATH + '/'
        self.INTERMEDIATE_PATH      = INTERMEDIATE_PATH
        self.OUTPUT_PATH            = OUTPUT_PATH
        self.PART_ID                = PART_ID
        self.DATE                   = DATE
        self.USER_ID                = USER_ID
        self.FLOOR_ID_DICT          = FLOOR_ID_DICT
        self.ENABLE_GROUP_MODE      = ENABLE_GROUP_MODE
        self.ENABLE_OLD_PATH_POLICY = ENABLE_OLD_PATH_POLICY
        self.MAX_WORKERS            = MAX_WORKERS
        
        # メモ：
        # delta lake形式のデータテーブルに対応しようとした結果、余りにも大きいIO待ち時間が発生するようになってしまった
        # これの対処のための方法論の一つとして、ThreadPoolExecutorを利用した非同期処理を行う事とした
        self.executor = ThreadPoolExecutor(max_workers=self.MAX_WORKERS)
        self.futures  = []
        
    
    def debug_write_csv_file(self, SPLIT_MODE:Literal['ALL', 'SEPARATE'], section_id:str, file_path:str, df:pd.DataFrame, f_name:str, need_index:bool=False) -> None:
        PATH = self.DATABRICKS_PREFIX + self.BASE_PATH
        PATH = PATH + self.INTERMEDIATE_PATH + f'part_id={self.PART_ID}/{SPLIT_MODE}/'
        if SPLIT_MODE == 'ALL':
            PATH = PATH + file_path
        else:
            PATH = PATH + f'section_id={section_id}/' + file_path
        
        # IO bound taskをexecutorで実行
        future = self.executor.submit(ex.write_csv_file_noSpark, PATH, df, self.DATE, f_name, need_index)
        self.futures.append(future)
        
        return None
    
    def write_popular_area(self, SPLIT_MODE:Literal['ALL', 'SEPARATE'], section_id:str, GRANULARITY:Literal['daily', 'hourly'], file_path:str, df:pd.DataFrame, f_name:str) -> None:
        PATH = self.BASE_PATH + self.OUTPUT_PATH + file_path
        
        pick_col = 'unit_id'
        if self.ENABLE_GROUP_MODE:
            pick_col = 'network_id'
        df = df.rename(columns={'unique_id': pick_col})
        
        
        # BIチーム(筒井さん)の要請により、人気エリアの解析結果にカラムを追加することが決定した
        df['user_id']    = self.USER_ID
        df['place_id']   = self.PART_ID
        df['floor_name'] = df[pick_col].map(self.FLOOR_ID_DICT)
        
        
        if GRANULARITY == 'daily':
            sel_list    = ['date',         'user_id', 'place_id', 'floor_name', pick_col, 'popular_area']
            sort_list   = ['date',         'user_id', 'place_id', 'floor_name', pick_col]
            
        elif GRANULARITY == 'hourly':
            sel_list    = ['date', 'hour', 'user_id', 'place_id', 'floor_name', pick_col, 'popular_area']
            sort_list   = ['date', 'hour', 'user_id', 'place_id', 'floor_name', pick_col]
            
        else:
            raise ValueError("GRANULARITY is not correct.")
        
        # データフレームの整形と並び替え
        df = df[sel_list].sort_values(by=sort_list, ascending=True)
        
        
        if self.ENABLE_OLD_PATH_POLICY:
            # 旧バージョンで利用されていたパスポリシーが有効なら
            PATH = self.DATABRICKS_PREFIX + PATH
            
            # 指定の型へ変換する
            if GRANULARITY == 'daily':
                schema_dict = {'date': str,              'user_id': str, 'place_id': str, 'floor_name': str, pick_col:str, 'popular_area':float}
                df          = df.astype(schema_dict)
            else:
                schema_dict = {'date': str, 'hour': int, 'user_id': str, 'place_id': str, 'floor_name': str, pick_col:str, 'popular_area':float}
                df          = df.astype(schema_dict)
            
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
            
            # IO bound taskをexecutorで実行
            future = self.executor.submit(ex.write_csv_file_with_date_noSpark, PATH, df, self.DATE, f_name)
            self.futures.append(future)
            
        else:
            # DELTA LAKEを使用する場合
            PATH = PATH + f'{SPLIT_MODE}/'
            
            # PySparkへの変換
            spark = SparkSession.getActiveSession()
            if GRANULARITY == 'daily':
                partition   = ['date',         'user_id', 'place_id', 'floor_name']
                sdf_schema  = types.StructType([
                                    types.StructField('date',         types.DateType(),   nullable=False),
                                    types.StructField('user_id',      types.StringType(), nullable=False),
                                    types.StructField('place_id',     types.StringType(), nullable=False),
                                    types.StructField('floor_name',   types.StringType(), nullable=False),
                                    types.StructField(pick_col,       types.StringType(), nullable=False),
                                    types.StructField('popular_area', types.DoubleType(), nullable=False),
                                ])
                
                df_pop_area = spark.createDataFrame(df)\
                                    .withColumn('date',         F.to_date(col('date'), 'yyyy-MM-dd'))\
                                    .withColumn('user_id',      col('user_id').cast(     types.StringType()))\
                                    .withColumn('place_id',     col('place_id').cast(    types.StringType()))\
                                    .withColumn('floor_name',   col('floor_name').cast(  types.StringType()))\
                                    .withColumn(pick_col,       col(pick_col).cast(      types.StringType()))\
                                    .withColumn('popular_area', col('popular_area').cast(types.DoubleType()))
            else:
                partition   = ['date', 'hour', 'user_id', 'place_id', 'floor_name']
                sdf_schema  = types.StructType([
                                    types.StructField('date',         types.DateType(),    nullable=False),
                                    types.StructField('hour',         types.IntegerType(), nullable=False),
                                    types.StructField('user_id',      types.StringType(),  nullable=False),
                                    types.StructField('place_id',     types.StringType(),  nullable=False),
                                    types.StructField('floor_name',   types.StringType(),  nullable=False),
                                    types.StructField(pick_col,       types.StringType(),  nullable=False),
                                    types.StructField('popular_area', types.DoubleType(),  nullable=False),
                                ])
                
                df_pop_area = spark.createDataFrame(df)\
                                    .withColumn('date',         F.to_date(col('date'), 'yyyy-MM-dd'))\
                                    .withColumn('hour',         col('hour').cast(        types.IntegerType()))\
                                    .withColumn('user_id',      col('user_id').cast(     types.StringType()))\
                                    .withColumn('place_id',     col('place_id').cast(    types.StringType()))\
                                    .withColumn('floor_name',   col('floor_name').cast(  types.StringType()))\
                                    .withColumn(pick_col,       col(pick_col).cast(      types.StringType()))\
                                    .withColumn('popular_area', col('popular_area').cast(types.DoubleType()))
            
            # IO bound taskをexecutorで実行
            future = self.executor.submit(ex.write_delta_lake_table, PATH, sdf_schema, partition, df_pop_area)
            self.futures.append(future)
        
        return None
    
    def __del__(self):
        all_success_flg = True
        # 全てのfutureが完了するまで待機
        for idx, future in enumerate(as_completed(self.futures)):
            res = future.result()
            if res:
                print(f"idx:{idx}  The specified data was successfully written.")
            else:
                print(f"idx:{idx}  Failed to write specified data.")
                all_success_flg = False
        
        # 終了のための後処理
        self.futures = []
        self.executor.shutdown(wait=True)
        if not all_success_flg:
            raise ValueError('Failed to write analysis data.')



# メモ：
# 当初は、AI BeaconとGPS Dataの対応の切り替えをANALYSIS_OBJECT変数によって実現していた
# しかしAI Beacon特有の要求やGPS Data特有の要求を一つのクラスで管理していたために、バグが頻発するようになった
# この反省として、AI BeaconとGPS Dataの対応の切り替えをクラスを分割することで対処することにした
class original_AIB_DL(download_to_file):
    def __init__(self, DATABRICKS_PREFIX:str, BASE_PATH:str, PREPROCESS_PATH:str, PART_ID:str, DATE:str) -> None:
        super().__init__()
        self.DATABRICKS_PREFIX = DATABRICKS_PREFIX
        self.BASE_PATH         = BASE_PATH + '/'
        self.PREPROCESS_PATH   = PREPROCESS_PATH
        self.PART_ID           = PART_ID
        self.DATE              = DATE
    
    def read_yaml_file(self, path:str) -> dict:
        PATH = self.DATABRICKS_PREFIX + self.BASE_PATH
        return im.read_yaml_file_noSpark(PATH + path)
    
    def read_csv_file(self, path:str) -> pd.DataFrame:
        PATH = self.BASE_PATH
        return im.read_csv_file(PATH + path)
        
    def read_parquet_for_1min(self, ueid_list:list[str]) -> pd.DataFrame:
        PATH    = self.BASE_PATH + self.PREPROCESS_PATH + 'by1min/'
        pd_1min = im.read_parquet_file(PATH, self.DATE)
        
        schema_by1min = {
            'date':        str,
            'minute':      str,
            'folder_name': str,
            'user_id':     str,
            'place_id':    str,
            'floor_name':  str,
            'network_id':  str,
            'unit_id':     str,
            '1min_count':  int,
        }
        
        pd_1min = pd_1min.astype(schema_by1min)
        pd_1min = pd_1min[pd_1min['unit_id'].isin(ueid_list)]
        pd_1min = pd_1min.rename(columns={'unit_id': 'unique_id'})
        pd_1min['minute'] = pd.to_datetime(pd_1min['minute'], format='%Y-%m-%d %H:%M:%S')
            
        
        return pd_1min