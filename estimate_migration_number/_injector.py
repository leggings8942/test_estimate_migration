from typing import Literal

from pyspark import StorageLevel
from pyspark.sql import DataFrame, types
from pyspark.sql.functions import col
import pyspark.sql.functions as F

from concurrent.futures import ThreadPoolExecutor, as_completed

import IO_control.export_file as ex
import IO_control.import_file as im
from UseCases._interface import calc_estimate_migration

# メモ：
# delta lake形式でのデータ管理について
# 型が自動で変換される等の挙動を見せるくせに、型変換がミスであるとカラムが破壊されるバグが存在する
# データの読み込み時にもこのバグは存在し、sparkはエラーを吐かない
# こちらが指定したスキーマと違うデータを読み込んだ場合にはエラーを吐かずに、カラムが破壊された状態で処理を続けようとする
# これらの挙動に対する対策は、扱うファイルのカラム名や型をそもそも間違わないようにするくらいしかない
# 出力元・出力先でsparkを扱う可能性があるファイルについては最新の注意を払い、カラム名・カラム順・カラム型の相違が発生しないようにすること



# メモ：
# 当初は、AI BeaconとGPS Dataの対応の切り替えをANALYSIS_OBJECT変数によって実現していた
# しかしAI Beacon特有の要求やGPS Data特有の要求を一つのクラスで管理していたために、バグが頻発するようになった
# この反省として、AI BeaconとGPS Dataの対応の切り替えをクラスを分割することで対処することにした
# AI Beacon専用 推定回遊人数計算用インターフェース
class original_AIB_EM(calc_estimate_migration):
    def __init__(self,
                 DATABRICKS_PREFIX:str,
                 BASE_PATH:str,
                 INTERMEDIATE_PATH:str,
                 OUTPUT_PATH:str,
                 PART_ID:str,
                 DATE:str,
                 ENABLE_GROUP_MODE:bool,
                 ENABLE_OLD_PATH_POLICY:bool,
                 MAX_WORKERS:int=20,
                ) -> None:
        super().__init__()
        self.DATABRICKS_PREFIX      = DATABRICKS_PREFIX
        self.BASE_PATH              = BASE_PATH + '/'
        self.INTERMEDIATE_PATH      = INTERMEDIATE_PATH
        self.OUTPUT_PATH            = OUTPUT_PATH
        self.PART_ID                = PART_ID
        self.DATE                   = DATE
        self.ENABLE_GROUP_MODE      = ENABLE_GROUP_MODE
        self.ENABLE_OLD_PATH_POLICY = ENABLE_OLD_PATH_POLICY
        self.MAX_WORKERS            = MAX_WORKERS
        
        
        # メモ：
        # delta lake形式のデータテーブルに対応しようとした結果、余りにも大きいIO待ち時間が発生するようになってしまった
        # これの対処のための方法論の一つとして、ThreadPoolExecutorを利用した非同期処理を行う事とした
        self.executor = ThreadPoolExecutor(max_workers=self.MAX_WORKERS)
        self.futures  = []
        
    
    def read_csv_file(self, path:str) -> DataFrame:
        PATH   = self.BASE_PATH + path
        df_csv = im.read_csv_file(PATH)
        return df_csv
    
    def read_visitor_number(self, migration_type:Literal['ALL_FLOOR', 'SEPARATE_FLOOR'], section_id:str, GRANULARITY:Literal['daily', 'hourly']) -> DataFrame:
        folder_path = self.BASE_PATH + self.INTERMEDIATE_PATH
        if migration_type == 'ALL_FLOOR':
            folder_path = folder_path + f'part_id={self.PART_ID}/ALL/'
        
        elif migration_type == 'SEPARATE_FLOOR':
            folder_path = folder_path + f'part_id={self.PART_ID}/SEPARATE/section_id={section_id}/'
            
        else:
            raise ValueError("migration_type is not correct.")
        
        
        if GRANULARITY == 'daily':
            folder_path = folder_path + 'daily/'
            ps_res = im.read_csv_file(folder_path + f'{self.DATE}_optmz4.csv')
            
        elif GRANULARITY == 'hourly':
            folder_path = folder_path + 'hourly/'
            ps_res = im.read_csv_file(folder_path + f'{self.DATE}_optmz4.csv')
            
        else:
            raise ValueError("GRANULARITY is not correct.")
        
        
        pick_col = 'unit_id'
        if self.ENABLE_GROUP_MODE:
            pick_col = 'network_id'
        
        
        if GRANULARITY == 'daily':
            sel_list = ['date',         'user_id', 'place_id', 'floor_name', pick_col, 'visitor_number']
        
        elif GRANULARITY == 'hourly':
            sel_list = ['date', 'hour', 'user_id', 'place_id', 'floor_name', pick_col, 'visitor_number']
            
        else:
            raise ValueError("GRANULARITY is not correct.")
        
        # データフレームの整形
        # ユニークID列の命名変更
        ps_res = ps_res.select(sel_list).withColumnRenamed(pick_col, 'ORIGIN')
        if GRANULARITY == 'daily':
            # 指定列の型の指定
            ps_res = ps_res\
                        .withColumn('date',           F.to_date(col('date'), 'yyyy-MM-dd'))\
                        .withColumn('user_id',        col('user_id').cast(       types.StringType()))\
                        .withColumn('place_id',       col('place_id').cast(      types.StringType()))\
                        .withColumn('floor_name',     col('floor_name').cast(    types.StringType()))\
                        .withColumn('ORIGIN',         col('ORIGIN').cast(        types.StringType()))\
                        .withColumn('visitor_number', col('visitor_number').cast(types.IntegerType()))
            
        elif GRANULARITY == 'hourly':
            # 指定列の型の指定
            ps_res = ps_res\
                        .withColumn('date',           F.to_date(col('date'), 'yyyy-MM-dd'))\
                        .withColumn('hour',           col('hour').cast(          types.IntegerType()))\
                        .withColumn('user_id',        col('user_id').cast(       types.StringType()))\
                        .withColumn('place_id',       col('place_id').cast(      types.StringType()))\
                        .withColumn('floor_name',     col('floor_name').cast(    types.StringType()))\
                        .withColumn('ORIGIN',         col('ORIGIN').cast(        types.StringType()))\
                        .withColumn('visitor_number', col('visitor_number').cast(types.IntegerType()))
        
        return ps_res
    
    def read_causality(self, migration_type:Literal['ALL_FLOOR', 'SEPARATE_FLOOR'], section_id:str, GRANULARITY:Literal['daily', 'hourly'], level:Literal[2, 3, 4]) -> DataFrame:
        folder_path = self.BASE_PATH + self.INTERMEDIATE_PATH
        if migration_type == 'ALL_FLOOR':
            folder_path = folder_path + f'part_id={self.PART_ID}/ALL/'
        
        elif migration_type == 'SEPARATE_FLOOR':
            folder_path = folder_path + f'part_id={self.PART_ID}/SEPARATE/section_id={section_id}/'
            
        else:
            raise ValueError("migration_type is not correct.")
        
        
        if GRANULARITY == 'daily':
            folder_path = folder_path + 'daily/'
            ps_res = im.read_csv_file(folder_path + f'{self.DATE}_migration_{level}.csv')
            
        elif GRANULARITY == 'hourly':
            folder_path = folder_path + 'hourly/'
            ps_res = im.read_csv_file(folder_path + f'{self.DATE}_migration_{level}.csv')
            
        else:
            raise ValueError("GRANULARITY is not correct.")
        
        
        if   level == 2:
            sel_list = ['date', 'ORIGIN',                   'DESTINATION', 'MOVEMENT_INFLUENCE_AMOUNT']
            
        elif level == 3:
            sel_list = ['date', 'ORIGIN', 'VIA_1',          'DESTINATION', 'MOVEMENT_INFLUENCE_AMOUNT_VIA_1']
            
        elif level == 4:
            sel_list = ['date', 'ORIGIN', 'VIA_1', 'VIA_2', 'DESTINATION', 'MOVEMENT_INFLUENCE_AMOUNT_VIA_2']
            
        else:
            raise ValueError("level is not correct.")
        
        
        # データフレームの整形
        ps_res = ps_res.select(sel_list)
        if   level == 2:
            # 指定列の型の指定
            ps_res = ps_res\
                        .withColumn('ORIGIN',                          col('ORIGIN').cast(                         types.StringType()))\
                        .withColumn('DESTINATION',                     col('DESTINATION').cast(                    types.StringType()))\
                        .withColumn('MOVEMENT_INFLUENCE_AMOUNT',       col('MOVEMENT_INFLUENCE_AMOUNT').cast(      types.DoubleType()))
            
        elif level == 3:
            # 指定列の型の指定
            ps_res = ps_res\
                        .withColumn('ORIGIN',                          col('ORIGIN').cast(                         types.StringType()))\
                        .withColumn('VIA_1',                           col('VIA_1').cast(                          types.StringType()))\
                        .withColumn('DESTINATION',                     col('DESTINATION').cast(                    types.StringType()))\
                        .withColumn('MOVEMENT_INFLUENCE_AMOUNT_VIA_1', col('MOVEMENT_INFLUENCE_AMOUNT_VIA_1').cast(types.DoubleType()))
            
        elif level == 4:
            # 指定列の型の指定
            ps_res = ps_res\
                        .withColumn('ORIGIN',                          col('ORIGIN').cast(                         types.StringType()))\
                        .withColumn('VIA_1',                           col('VIA_1').cast(                          types.StringType()))\
                        .withColumn('VIA_2',                           col('VIA_2').cast(                          types.StringType()))\
                        .withColumn('DESTINATION',                     col('DESTINATION').cast(                    types.StringType()))\
                        .withColumn('MOVEMENT_INFLUENCE_AMOUNT_VIA_2', col('MOVEMENT_INFLUENCE_AMOUNT_VIA_2').cast(types.DoubleType()))
        
        
        if GRANULARITY == 'daily':
            # date列の日付型への変更
            ps_res = ps_res\
                        .withColumn('date', F.to_date(col('date'), 'yyyy-MM-dd'))
            
        else:
            # date列のtimestamp型への変換
            # hour列の追加
            # date列の日付型への変更
            ps_res = ps_res\
                        .withColumn('date', F.to_timestamp(col('date'), 'yyyy-MM-dd HH:mm:ss'))\
                        .withColumn('hour', F.hour(col('date')))\
                        .withColumn('date', F.to_date(col('date')))
        
        return ps_res
    
    def write_estimate_migration_number(self, migration_type:Literal['ALL_FLOOR', 'SEPARATE_FLOOR'], section_id:str, GRANULARITY:Literal['daily', 'hourly'], level:Literal[2, 3, 4], df_move_pop:DataFrame) -> None:
        PATH        = self.BASE_PATH + self.OUTPUT_PATH
        folder_path = f'推定回遊人数{level}階層/'
        if GRANULARITY == 'daily':
            folder_path = folder_path + 'daily/'
            
        elif GRANULARITY == 'hourly':
            folder_path = folder_path + 'hourly/'
        
        else:
            raise ValueError("GRANULARITY is not correct.")
        
        
        if   level == 2:
            tmp_list = []
            
        elif level == 3:
            tmp_list = ['VIA_1']
            
        elif level == 4:
            tmp_list = ['VIA_1', 'VIA_2']
            
        else:
            raise ValueError("level is not correct.")
        
    
        if GRANULARITY == 'daily':
            sel_list  = ['date',         'user_id', 'place_id', 'floor_name', 'ORIGIN', *tmp_list, 'DESTINATION', 'move_pop']
            sort_list = ['date',         'user_id', 'place_id', 'floor_name', 'ORIGIN', *tmp_list, 'DESTINATION']
            partition = ['date',         'user_id', 'place_id', 'floor_name']
            
            sdf_schema  = types.StructType([
                types.StructField('date',        types.DateType(),   nullable=False),
                types.StructField('user_id',     types.StringType(), nullable=False),
                types.StructField('place_id',    types.StringType(), nullable=False),
                types.StructField('floor_name',  types.StringType(), nullable=False),
                types.StructField('ORIGIN',      types.StringType(), nullable=False),
                *[types.StructField(column,      types.StringType(), nullable=False) for column in tmp_list],
                types.StructField('DESTINATION', types.StringType(), nullable=False),
                types.StructField('move_pop',    types.DoubleType(), nullable=False),
            ])
            
            # データフレームの整列
            # 指定列でのソート
            # 指定列の型の指定
            df_move_pop = df_move_pop\
                                .select(sel_list)\
                                .sort(sort_list)\
                                .withColumn('date',        F.to_date(col('date'), 'yyyy-MM-dd'))\
                                .withColumn('user_id',     col('user_id').cast(    types.StringType()))\
                                .withColumn('place_id',    col('place_id').cast(   types.StringType()))\
                                .withColumn('floor_name',  col('floor_name').cast( types.StringType()))\
                                .withColumn('ORIGIN',      col('ORIGIN').cast(     types.StringType()))\
                                .withColumn('DESTINATION', col('DESTINATION').cast(types.StringType()))\
                                .withColumn('move_pop',    col('move_pop').cast(   types.DoubleType()))
            
            if   level == 3:
                df_move_pop = df_move_pop.withColumn('VIA_1', col('VIA_1').cast(types.StringType()))
            
            elif level == 4:
                df_move_pop = df_move_pop.withColumn('VIA_1', col('VIA_1').cast(types.StringType()))\
                                        .withColumn( 'VIA_2', col('VIA_2').cast(types.StringType()))
            
            
        elif GRANULARITY == 'hourly':
            sel_list  = ['date', 'hour', 'user_id', 'place_id', 'floor_name', 'ORIGIN', *tmp_list, 'DESTINATION', 'move_pop']
            sort_list = ['date', 'hour', 'user_id', 'place_id', 'floor_name', 'ORIGIN', *tmp_list, 'DESTINATION']
            partition = ['date', 'hour', 'user_id', 'place_id', 'floor_name']
            
            sdf_schema  = types.StructType([
                types.StructField('date',        types.DateType(),    nullable=False),
                types.StructField('hour',        types.IntegerType(), nullable=False),
                types.StructField('user_id',     types.StringType(),  nullable=False),
                types.StructField('place_id',    types.StringType(),  nullable=False),
                types.StructField('floor_name',  types.StringType(),  nullable=False),
                types.StructField('ORIGIN',      types.StringType(),  nullable=False),
                *[types.StructField(column,      types.StringType(),  nullable=False) for column in tmp_list],
                types.StructField('DESTINATION', types.StringType(),  nullable=False),
                types.StructField('move_pop',    types.DoubleType(),  nullable=False),
            ])
            
            # データフレームの整列
            # 指定列でのソート
            # 指定列の型の指定
            df_move_pop = df_move_pop\
                                .select(sel_list)\
                                .sort(sort_list)\
                                .withColumn('date',        F.to_date(col('date'), 'yyyy-MM-dd'))\
                                .withColumn('hour',        col('hour').cast(       types.IntegerType()))\
                                .withColumn('user_id',     col('user_id').cast(    types.StringType()))\
                                .withColumn('place_id',    col('place_id').cast(   types.StringType()))\
                                .withColumn('floor_name',  col('floor_name').cast( types.StringType()))\
                                .withColumn('ORIGIN',      col('ORIGIN').cast(     types.StringType()))\
                                .withColumn('DESTINATION', col('DESTINATION').cast(types.StringType()))\
                                .withColumn('move_pop',    col('move_pop').cast(   types.DoubleType()))
            
            if   level == 3:
                df_move_pop = df_move_pop.withColumn('VIA_1', col('VIA_1').cast(types.StringType()))
            
            elif level == 4:
                df_move_pop = df_move_pop.withColumn('VIA_1', col('VIA_1').cast(types.StringType()))\
                                        .withColumn( 'VIA_2', col('VIA_2').cast(types.StringType()))
        
        else:
            raise ValueError("GRANULARITY is not correct.")
        
        
        # 遅延評価をここで確定させる
        df_move_pop.persist(StorageLevel.MEMORY_ONLY)
        df_move_pop.count()
        
        if self.ENABLE_OLD_PATH_POLICY:
            # 旧バージョンで利用されていたパスポリシーが有効なら
            folder_path = PATH + folder_path
            
            # IO bound taskをexecutorで実行
            future = self.executor.submit(ex.write_parquet_file, self.DATABRICKS_PREFIX, folder_path, self.DATE, df_move_pop)
            self.futures.append(future)
        
        else:
            # DELTA LAKEを使用する場合
            if migration_type == 'ALL_FLOOR':
                folder_path = folder_path + f'ALL/'
        
            elif migration_type == 'SEPARATE_FLOOR':
                folder_path = folder_path + f'SEPARATE/'
        
            else:
                raise ValueError("migration_type is not correct.")
            folder_path = PATH + folder_path
            
            
            # IO bound taskをexecutorで実行
            future = self.executor.submit(ex.write_delta_lake_table, folder_path, sdf_schema, partition, df_move_pop)
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
    