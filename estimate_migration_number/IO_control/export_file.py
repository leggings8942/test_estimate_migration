import os
import time
from pyspark.sql import SparkSession, DataFrame, types
from delta.tables import DeltaTable

# メモ：
# DatabricksはファイルIOの処理が苦手であることがわかった
# Pandasも遅いが、PySparkはさらに輪をかけて遅い
# ファイルの読み込みは処理の完了を待つしかないが、書き込みは完了を待つ必要がない
# なぜなら、後続の処理で書き込みファイルに対する処理を行なっていないためである
# そのため並列実行によってIO Boundな処理はサブブロセスに投げる事とした


# メモ：
# delta lake形式のデータ管理を行い、かつ並列処理を行ったところ、確率的にnullが代入されることがわかった
# 実際には別の原因が理由でdelta lake形式のデータにnullが代入されているのかもしれないが、今の所理解できていない
# そのため、delta lakeデータのスキーマに対してNOT NULL制約を付加することにした
# しかし、sparkの仕様としてpyspark api経由ではスキーマに対してNOT NULL制約を付加することができないことが判明した
# このことからSQL文経由でのスキーマ宣言を行う事となったが、sparkの仕様としてスキーマ宣言だけでは物理メモリに対して何ら影響を及ぼさないことが判明した
# 並列処理によって当該のdelta lakeデータが存在しているかを判定している以上、この仕様はとても不都合である
# 以上の理由から、スキーマ宣言を行った後で空データを書き込むことで他プロセスに当該のdelta lekeデータが存在していることを知らせる事とした


def spark_schema_to_sql(schema:types.StructType) -> str:
    # PySpark Type → SQL Type への変換辞書
    type_dict = {
        types.DateType():      'DATE',
        types.TimestampType(): 'TIMESTAMP',
        types.IntegerType():   'INTEGER',
        types.LongType():      'BIGINT',
        types.FloatType():     'FLOAT',
        types.DoubleType():    'DOUBLE',
        types.StringType():    'STRING',
        types.BinaryType():    'BINARY',
        types.BooleanType():   'BOOLEAN',
    }
    
    sql_fields = []
    for field in schema.fields:
        # 指定スキーマの分解
        field_name     = field.name
        field_type     = field.dataType
        field_nullable = field.nullable
        
        # SQLへの変換
        sql_name     = field_name
        sql_type     = type_dict[field_type]
        sql_nullable = '' if field_nullable else 'NOT NULL'
        
        sql_fields.append(f"{sql_name}  {sql_type} {sql_nullable}")
    
    sql_schema = ',\n'.join(sql_fields)
    
    return sql_schema


def write_parquet_file(DATABRICKS_PREFIX:str, OUTPUT_PATH:str, date:str, df:DataFrame) -> bool:
    folder_path = OUTPUT_PATH + f'year={date[0:4]}/month={date[0:7]}/date={date}/'
    # パス先がなければディレクトリの作成
    if not os.path.isdir(DATABRICKS_PREFIX + folder_path): 
        os.makedirs(DATABRICKS_PREFIX + folder_path, exist_ok=True)
    # csv出力
    df.coalesce(1)\
        .write\
        .mode('overwrite')\
        .option('header',      'True')\
        .option('compression', 'snappy')\
        .parquet(folder_path)
    
    df.unpersist(blocking=True)
    return True


# メモ：
# この関数はThreadPoolExecutorによる並列実行を想定して設計している
# この関数に代入されるSpark FrameWorkの永続性を解除することに違和感を覚えるかもしれないが、これは仕様である
# 不要になったFrameWorkのリソースを即時解放しないと、後続の処理に対するパフォーマンスが著しく悪化するためである
def write_delta_lake_table(OUTPUT_PATH:str, table_schema:types.StructType, partition_key:list, df:DataFrame) -> bool:
    
    # メモ；
    # 空のデータに対してdf.write.format('delta').mode('overwrite')を行うとエラーになることがわかった
    # 'append'も同様である
    # エラー回避のためデータが空の場合には書き込まない
    if df.count() == 0:
        print("DataFrame is empty, skipping write operation")
        df.unpersist(blocking=True)
        return True
    
    spark = SparkSession.getActiveSession()
    
    
    # メモ：
    # 初回書き込み時に大量の並列JOBがテーブルの初回作成を行おうとするとエラーが発生する場合がある
    # ProtocolChangedException: [DELTA_PROTOCOL_CHANGED] ProtocolChangedException: The protocol version of the Delta table has been changed by a concurrent update. This happens when multiple writers are writing to an empty directory. Creating the table ahead of time will avoid this conflict. 
    # これの回避のためにリトライロジックを組み込む事とする
    
    for _ in range(5):
        try:
            if not DeltaTable.isDeltaTable(spark, OUTPUT_PATH):
                # そもそも既存のdeltalake形式のデータが存在しない場合には、明示的にテーブルを作成する
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS delta.`{OUTPUT_PATH}` (
                        {spark_schema_to_sql(table_schema)}
                    )
                    USING DELTA
                    PARTITIONED BY ({', '.join(partition_key)})
                """)
                
                # 他プロセスへ知らせるため・デバッグを容易にするために空データの作成・挿入
                empty_df = spark.createDataFrame([], schema=table_schema)
                empty_df.write\
                        .format('delta')\
                        .mode('append')\
                        .save(OUTPUT_PATH)
                
            
            # 既存のデルタ形式のデータに対する削除条件        
            replace_conditions = []
            for row in df.select(partition_key).dropDuplicates().collect():
                tmp = " AND ".join([f"{key} = '{row[key]}'" for key in partition_key])
                replace_conditions.append(f"({tmp})")
        
            replace_conditions = " OR ".join(replace_conditions)
        
            # 新しいデータを追加
            df.write\
                .format('delta')\
                .mode('overwrite')\
                .option('compression', 'snappy')\
                .option('replaceWhere', replace_conditions)\
                .save(OUTPUT_PATH)
        
        except Exception as e:
            print(f"Failed to write Delta table: {e}")
            time.sleep(60)
        else:
            # 成功
            df.unpersist(blocking=True)
            return True
    
    df.unpersist(blocking=True)
    return False

