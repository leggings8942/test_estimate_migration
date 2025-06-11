import numpy as np
import pandas as pd
import networkx as nx
import itertools

from ._interface import time_series_model


'''
インパルス応答関数の出力を因果量に変換する関数
'''
def convert_irf2cau(lag:int, unit_list:list, result_irf:np.ndarray[np.float64]) -> dict:
    tuple_dic = {}
    tmp_irf   = result_irf[lag]
    for idx_from, idx_to in itertools.product(range(0, len(unit_list)), repeat=2):
        irf_tmp   = round(tmp_irf[idx_to][idx_from], 4)
        from_unit = unit_list[idx_from]
        to_unit   = unit_list[idx_to]

        # 因果量が正かつ自己回帰でない場合に登録する
        # グラフにおける辺の追加。因果量が負で自己回帰するものは辺として含めない。
        if irf_tmp > 0 and not(from_unit == to_unit):
            tuple_dic[from_unit, to_unit] = irf_tmp
        else: # 因果量が負または自己回帰であれば0とする
            tuple_dic[from_unit, to_unit] = 0

    return tuple_dic


'''
インパルス応答関数の出力を次数中心性に変換する関数
'''
def convert_irf2cen(lag:int, unit_list:list, sum_list:list, result_irf:np.ndarray[np.float64]) -> nx.DiGraph:
    centrality = nx.DiGraph()
    for beacon, s in zip(unit_list, sum_list):
        centrality.add_node(beacon, count=s)
    
    tmp_irf = result_irf[lag]
    for idx_from, idx_to in itertools.product(range(0, len(unit_list)), repeat=2):
        irf_tmp   = round(tmp_irf[idx_to][idx_from], 4)
        from_unit = unit_list[idx_from]
        to_unit   = unit_list[idx_to]

        # 因果量が正かつ自己回帰でない場合に登録する
        # グラフにおける辺の追加。因果量が負で自己回帰するものは辺として含めない。
        if irf_tmp > 0 and not(from_unit == to_unit):
            centrality.add_edge(from_unit, to_unit, weight=irf_tmp)

    return centrality


'''
インパルス応答関数に使用された学習係数のフレームワークを作成する関数
'''
def output_debug_coef(lag:int, unit_list:list, intercept:np.ndarray[np.float64], coefficient:np.ndarray[np.float64]) -> tuple[pd.DataFrame, pd.DataFrame]:
    temp_list  = []
    temp_list2 = []
    sort_list  = sorted(unit_list)
    for i in range(1, lag + 1):
        temp_list  = temp_list  + list(map(lambda x: f'{x}_lag{i}', unit_list))
        temp_list2 = temp_list2 + list(map(lambda x: f'{x}_lag{i}', sort_list))
  
    # デバッグ用データフレーム作成
    pd_intercept   = pd.DataFrame(intercept.T,   index=unit_list, columns=['intercept'])
    pd_coefficient = pd.DataFrame(coefficient.T, index=unit_list, columns=temp_list)

    # 外生性の担保のため簡易的にソートされた部分を、元に戻す
    pd_intercept   = pd_intercept.sort_index()
    pd_coefficient = pd_coefficient.sort_index()[temp_list2]

    return pd_intercept, pd_coefficient


'''
インパルス応答関数の計算結果を返す関数
'''
def get_irf(calc_model:time_series_model, df:pd.DataFrame, lag=10, period=1) -> tuple[np.ndarray[np.float64], tuple[list, pd.DataFrame, pd.DataFrame]]:
    # 時系列データを大きい順に並べる
    df_tmp    = df.fillna(0).sort_index(ascending=True)
    df_sorted = df_tmp[df_tmp.mean(axis='index').sort_values(ascending=False).index]
    unit_list = list(df_sorted.columns)
    
    # VARモデル作成のためのデータフレームの登録
    calc_model.register(df_sorted)
    
    # VARモデルを最大ラグ10でハンナン=クイン情報量基準で最適なモデルを出力する
    lags = calc_model.select_order(maxlag=lag, ic='hqic')
    
    # 1期先のインパルス応答関数
    result_irf = calc_model.irf(period=period, orth=True, isStdDevShock=False)
    
    # デバッグのためVARモデルの学習係数を取得
    ver_i, ver_c = calc_model.get_coef()
    intercept, coefficient = output_debug_coef(lags, unit_list, ver_i, ver_c)
    
    return result_irf, (unit_list, intercept, coefficient)


'''
因果量の計算結果を返す関数
'''
def get_causality(calc_model:time_series_model, df:pd.DataFrame, lag=10, period=1) -> tuple[pd.DataFrame, tuple[pd.DataFrame, pd.DataFrame]]:
    # IRFの計算結果を取得
    result_irf, (unit_list, intercept, coefficient) = get_irf(calc_model, df, lag, period)
    
    # インパルス応答関数の出力を因果量に変換
    tuple_dic = convert_irf2cau(period, unit_list, result_irf)
    
    # 因果量のデータフレームを作る
    # [[30713, 30713, 0.0], [30713, 30714, 3.099], ...]
    # [[from, to, irf], ...]というリストを作る
    list_causality = [[pair[0], pair[1], causality] for pair, causality in tuple_dic.items()]
    df_causality   = pd.DataFrame(list_causality, columns=['ORIGIN', 'DESTINATION', 'MOVEMENT_INFLUENCE_AMOUNT'])
    df_causality   = df_causality.sort_values(by=['ORIGIN', 'DESTINATION'], ascending=True, ignore_index=True)
    return df_causality, (intercept, coefficient)


'''
人気度の計算結果を返す関数
'''
def get_popularity(calc_model:time_series_model, df:pd.DataFrame, lag=10, period=1) -> tuple[pd.DataFrame, tuple[pd.DataFrame, pd.DataFrame]]:
    # IRFの計算結果を取得
    result_irf, (unit_list, intercept, coefficient) = get_irf(calc_model, df, lag, period)
    
    # 各列の総合計値を取得
    sum_list = df[unit_list].fillna(0).sum(axis=0).tolist()
    
    # インパルス応答関数の出力を次数中心性に変換
    centrality = convert_irf2cen(period, unit_list, sum_list, result_irf)
    
    # 次数中心性のデータフレームを作る
    # {30865:0.8, 30866:1.3, ...}
    # 次数中心性を計算する
    dict_centrality = nx.degree_centrality(centrality)
    # [[30865, 0.8], [30866, 1.3], [30867, 0], ...]
    # 次数中心がある場合は値を入れるが無い場合は0を入れる
    ls2d_centrality = [[beacon, round(dict_centrality[beacon], 4) if beacon in dict_centrality else 0] for beacon in unit_list]

    df_centrality = pd.DataFrame(ls2d_centrality, columns=['unique_id', 'popular_area'])
    df_centrality = df_centrality.sort_values(by='unique_id', ascending=True, ignore_index=True)
    return df_centrality, (intercept, coefficient)


'''
因果量・人気度の計算結果を返す関数
'''
def get_influence(calc_model:time_series_model, df:pd.DataFrame, lag=10, period=1) -> tuple[pd.DataFrame, pd.DataFrame, tuple[pd.DataFrame, pd.DataFrame]]:
    # IRFの計算結果を取得
    result_irf, (unit_list, intercept, coefficient) = get_irf(calc_model, df, lag, period)
    
    # #############################################################
    # 因果量計算部
    # #############################################################
    # インパルス応答関数の出力を因果量に変換
    tuple_dic = convert_irf2cau(period, unit_list, result_irf)
    
    # 因果量のデータフレームを作る
    # [[30713, 30713, 0.0], [30713, 30714, 3.099], ...]
    # [[from, to, irf], ...]というリストを作る
    list_causality = [[pair[0], pair[1], causality] for pair, causality in tuple_dic.items()]
    df_causality   = pd.DataFrame(list_causality, columns=['ORIGIN', 'DESTINATION', 'MOVEMENT_INFLUENCE_AMOUNT'])
    df_causality   = df_causality.sort_values(by=['ORIGIN', 'DESTINATION'], ascending=True, ignore_index=True)
    
    
    # #############################################################
    # 人気度計算部
    # #############################################################
    # 各列の総合計値を取得
    sum_list = df[unit_list].fillna(0).sum(axis=0).tolist()
    
    # インパルス応答関数の出力を次数中心性に変換
    centrality = convert_irf2cen(period, unit_list, sum_list, result_irf)
    
    # 次数中心性のデータフレームを作る
    # {30865:0.8, 30866:1.3, ...}
    # 次数中心性を計算する
    dict_centrality = nx.degree_centrality(centrality)
    # [[30865, 0.8], [30866, 1.3], [30867, 0], ...]
    # 次数中心がある場合は値を入れるが無い場合は0を入れる
    ls2d_centrality = [[beacon, round(dict_centrality[beacon], 4) if beacon in dict_centrality else 0] for beacon in unit_list]
    
    df_centrality = pd.DataFrame(ls2d_centrality, columns=['unique_id', 'popular_area'])
    df_centrality = df_centrality.sort_values(by='unique_id', ascending=True, ignore_index=True)
    
    return df_causality, df_centrality, (intercept, coefficient)