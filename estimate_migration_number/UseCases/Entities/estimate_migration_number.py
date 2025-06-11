from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as F


'''
このモデルを構築するにあたり、参考にした過去案件がある
・下水サーベイランス
  2022年ごろから本格的に大流行したCOVID-19について新規陽性者数の予測モデルを構築して欲しいという案件を、内閣感染症危機管理統括庁より依頼された。
  この案件に取り掛かるにあたり、以下の二つのモデルの構築を行なった。
  ・人流観測量から下水中のウイルスRNA濃度を予測するモデル
  ・下水中のウイルスRNA濃度から新規陽性者数を予測するモデル
  
  以下にこの案件についての概要・報告書がまとめてある参考URLを示す。
  https://www.caicm.go.jp/citizen/corona/survey/surveillance.html
  https://www.pref.kyoto.jp/digital/diseasex/documents/surveillance.pdf


その時に構築されたモデル(人流観測量から下水中のウイルスRNA濃度を予測するモデル)をここに示す
E = M C^2
下水中のウイルスRNA濃度の推定には、上述の数式を理論的な根拠としている
特殊相対性理論における質量エネルギー等価式である

それぞれの変数に対して、独自に意味定義を行うことで推定モデルを構築する
E:エネルギー → IRF(日別の人流観測量からVARモデルを構築して計算)
M:質量     → 下水中のウイルスRNA濃度
C:光速     → 人流観測量

処理手順.
1. C^2 = 人流観測量^2 を計算する
2. M = E / (C^2) = IRF / (人流観測量^2) を計算する

このモデルは以下を前提としていることに注意してほしい
・質量エネルギー等価式で近似が可能
・事前に日毎のウイルスRNA濃度・人流観測量・IRFが把握できている

↓

推定回遊人数の予測モデルを構築するにあたり、過去に構築したモデルの理論背景を参考にすることにした。
以下に、推定回遊人数用にカスタマイズされた予測モデルをここに示す。

それぞれの変数に対して、独自に意味定義を行うことで推定モデルを構築する
E:エネルギー → IRF(因果量)
M:質量     → 推定移動人数の比率
C:光速     → 推定来訪者数の逆数の平方根

算出したMの値を、直接的に移動人数として扱うのではなく、あくまで比率として扱う
最終的な移動人数は、以下の数式で算出する
推定移動人数 = 推定来訪者数 * (M / ΣM)

処理手順.
1. C^2 = 1 / 推定来訪者数 を計算する
2. M = E / (C^2) = IRF * 推定来訪者数 を計算する
3. M の分母を計算する. この時に出発地点(ORIGIN)を基準に計算する
4. 推定移動人数 = 推定来訪者数 * (M / (M の分母))

このモデルは以下を前提としていることに注意してほしい
・事前に出発地点ごとの人数が把握できている
・事前に特定のエリアAからエリアBに向けたIRF(因果量)が把握できている
・ORIGINとDESTINATIONは同集合である
・解析対象エリア外(ORIGIN,DESTINATION以外の場所)に出る人はいない
・解析対象エリア外(ORIGIN,DESTINATION以外の場所)から入る人はいない

2025/04/28 - 追記:梛野裕也
元々の推定回遊人数のアルゴリズムから多少の変更を加えた
変更点を以下に示す
・C^2 = 1 / 推定来訪者数 とした
・推定来訪者数の基準を出発地点とした
'''

'''
C-valueを計算する関数
C-valueは、推定来訪者数の逆数である
'''
def calc_c_val(df:DataFrame) -> DataFrame:
    res = df.withColumn(
                'C-value',
                F.when(col('visitor_number') > 0, 1 / col('visitor_number')).otherwise(F.lit(1))
            )
    return res

'''
M-valueを計算する関数
M-valueは、IRF(E-value)をC-valueで割ったものである
'''
def create_move_val(key_list:str | list[str], influence:str, df_caus:DataFrame, df_c_val:DataFrame) -> DataFrame:
    res = df_caus\
            .join(df_c_val, on=key_list, how='inner')\
            .withColumn('move_val', col(influence) / col('C-value'))
    return res

'''
M-valueの分母を計算する関数
M-valueの分母は、指定の基準で集計した移動影響量の合計値である
'''
def create_move_val_total(group_list:str | list[str], df_move_val:DataFrame) -> DataFrame:
    res = df_move_val\
            .groupBy(group_list)\
            .sum()\
            .withColumn('sum(move_val)', F.when(col('sum(move_val)') == 0, F.lit(1)).otherwise(col('sum(move_val)')))
    return res

'''
M-valueを基準にした移動人数を計算する関数
推定移動人数は、推定来訪者数 * (M-value / ΣM-value)である
'''
def create_move_pop(key_list:str | list[str], df_move_val:DataFrame, df_visit_cnt:DataFrame, df_move_val_total:DataFrame) -> DataFrame:    
    res = df_move_val\
            .join(df_visit_cnt,      on=key_list, how='inner')\
            .join(df_move_val_total, on=key_list, how='inner')\
            .withColumn('move_pop', col('visitor_number') * (col('move_val') / col('sum(move_val)')))
    return res

'''
推定移動人数が1に満たない行を削除する関数
推定移動人数が1に満たない行は、移動人数として扱わない
'''
def remove_move_pop(df_move_pop:DataFrame) -> DataFrame:
    res = df_move_pop\
            .filter(col('move_pop') >= 1)
    return res

'''
日別推定移動人数を計算して返す関数
日別推定移動人数は、日別推定来訪者数と日別因果量を元に計算する
'''
def get_estimate_migration_number_daily(influence:str, df_visitor:DataFrame, df_causality:DataFrame) -> DataFrame:    
    # 各値の逆数をとる(Covid-19におけるRNAに合わせ逆数をとる)
    df_c_val = calc_c_val(df_visitor)
    df_c_val = df_c_val.select(['ORIGIN', 'C-value'])
    
    # 「O→D」因果量を「Oの時系列データにおける各時刻の逆数」で割る。(E=因果量, M=移動人数, C=推定人数の逆数)
    # →→→時間単位における「O→D」の人数を得る
    df_move_val = create_move_val('ORIGIN', influence, df_causality, df_c_val)
    
    # 「move_val」の分母を計算する
    df_move_val_total = create_move_val_total('ORIGIN', df_move_val.select(['ORIGIN', 'move_val']))
    df_move_val_total = df_move_val_total.select(['ORIGIN', 'sum(move_val)'])
    
    # 「move_val」をORIGIN基準で割合にして「move_pop」にする
    df_move_pop  = create_move_pop('ORIGIN', df_move_val, df_visitor.select(['user_id', 'place_id', 'floor_name', 'ORIGIN', 'visitor_number']), df_move_val_total)
    
    # 「move_pop」が1に満たない行の削除
    # df_migrate_num = remove_move_pop(df_move_pop)
    # メモ：
    # Delta Lakeへの移行に伴い、データ容量に対する要求がゆるくなった
    # そのため、計算結果の削除は行わない方針へと変更された
    df_migrate_num = df_move_pop
    
    return df_migrate_num

'''
時間別推定移動人数を計算して返す関数
時間別推定移動人数は、時間別推定来訪者数と時間別因果量を元に計算する
'''
def get_estimate_migration_number_hourly(influence:str, df_visitor:DataFrame, df_causality:DataFrame) -> DataFrame:    
    # 各値の逆数をとる(Covid-19におけるRNAに合わせ逆数をとる)
    df_c_val = calc_c_val(df_visitor)
    df_c_val = df_c_val.select(['hour', 'ORIGIN', 'C-value'])
    
    # 「O→D」因果量を「Oの時系列データにおける各時刻の値の逆数」で割る。(E=因果量, M=移動人数, C=推定人数の逆数)
    # →→→時間単位における「O→D」の人数を得る
    df_move_val = create_move_val(['hour', 'ORIGIN'], influence, df_causality, df_c_val)
    
    # 「move_val」の分母を計算する
    df_move_val_total = create_move_val_total(['hour', 'ORIGIN'], df_move_val.select(['hour', 'ORIGIN', 'move_val']))
    df_move_val_total = df_move_val_total.select(['hour', 'ORIGIN', 'sum(move_val)'])
    
    # 「move_val」をORIGIN基準で割合にして「move_pop」にする
    df_move_pop  = create_move_pop(['hour', 'ORIGIN'], df_move_val, df_visitor.select(['hour', 'user_id', 'place_id', 'floor_name', 'ORIGIN', 'visitor_number']), df_move_val_total)
    
    # 「move_pop」が1に満たない行の削除
    # df_migrate_num = remove_move_pop(df_move_pop)
    # メモ：
    # Delta Lakeへの移行に伴い、データ容量に対する要求がゆるくなった
    # そのため、計算結果の削除は行わない方針へと変更された
    df_migrate_num = df_move_pop
    return df_migrate_num


