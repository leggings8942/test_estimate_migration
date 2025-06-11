import time
import pandas as pd

from .Entities.causality import get_influence, get_popularity, get_causality
from .Entities.continuous_migration import get_migration, get_migration_three
from .Entities.data_reconstruction import get_subsection_to_unique_dict
from ._interface import time_series_model
from ._interface import download_to_file
from ._interface import upload_to_file
from ._interface import UseInterface

from .get_data import get_time_series_data, extract_data_by_time



'''
日毎のネットワークモデル解析
人気エリア(Popular area)： 全フロア
影響量(Causal quantity)： 全フロア
'''
def nwm_daily_pALL_cALL(calc_model:time_series_model, use_dl:download_to_file, use_ul:upload_to_file, param_dict:dict) -> None:
    
    date             = param_dict['date']                               # 解析対象日
    part_id          = param_dict['part_id']                            # 解析対象のパートID
    ueid_list        = param_dict['part_to_unique_list']                # 解析対象のユニークIDリスト
    sn_id_dict       = param_dict['section_to_unique_dict']             # セクション単位のユニークID辞書
    sn_ssn_id_dict   = param_dict['subsection_to_unique_dict']          # サブセクション単位のユニークID辞書
    start_time       = param_dict['daily_start_time']                   # 日別開始時刻
    end_time         = param_dict['daily_end_time']                     # 日別終了時刻
    time_interval    = param_dict['daily_time_interval']                # 日別時間間隔
    migration_num    = param_dict['daily_estimate_migration_number'][1] # 必要回遊階層数
    flg_group        = param_dict['GROUP_MODE_FLAG']                    # グループモードフラグ
    flg_specify_time = param_dict['SPECIFY_TIME_FLAG']                  # 時間指定フラグ
    

    
    # ====================================================================
    # 解析対象時系列データ取得処理
    # ====================================================================
        
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ準備時間 計測開始")
    
    # 1分間隔時系列データの取得
    df = get_time_series_data(use_dl, date, ueid_list)
    
    # 時間指定が有効な場合
    if flg_specify_time:
        # start_time ~ end_time までのデータを抽出
        df = extract_data_by_time(df, date, start_time, end_time)
    
    # 指定時間隔計測に置き換える
    # [15:00, 15:05) labeled 15:00 → setting(rule='5min, closed='left', label='left')
    df_granul = df.set_index(keys='datetime').resample(rule=time_interval, axis=0, closed='left', label='left').sum()
        
    # グループモードが有効な場合
    if flg_group:
        # サブセクション毎にユニークIDを一まとめにする
        
        # サブセクション単位のユニークID辞書を取得
        ssn_id_dict = get_subsection_to_unique_dict(sn_ssn_id_dict)
        
        # サブセクション単位でデータを整形
        for subsection_id in ssn_id_dict.keys():
            # ユニークIDリストの取得
            unique_id_list = ssn_id_dict[subsection_id]
            
            # グループ単位に置き換える
            df_granul[subsection_id] = df_granul[unique_id_list].sum(axis='columns')
        
        # ユニークIDリストを削除
        df_granul = df_granul.drop(columns=ueid_list)
    
    # 時間計測完了
    time_end  = time.perf_counter()
    time_diff = time_end - time_start
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ準備時間 {time_diff}秒")
    
    # 解析対象の時系列データの出力
    tmp_path = 'daily/' + date + '_debug_data/'
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_granul, 'input_time_series_data', need_index=True)
    
    # ====================================================================
    # 解析対象時系列データ解析処理
    # ====================================================================
    
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ解析(人気エリア・因果量)時間 計測開始")
    
    # 1日単位ごとにNWM計算を行う
    df_output_causality, df_output_centrality, (df_debug_inte, df_debug_coef) = get_influence(calc_model, df_granul)
    
    # 時間計測完了
    time_end = time.perf_counter()
    time_diff  = time_end - time_start
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ解析(人気エリア・因果量)時間 {time_diff}秒")
    
    # ====================================================================
    # 解析対象時系列データ解析結果 出力処理
    # ====================================================================
    
    # データフレームの先頭にdateを追記する
    df_output_causality.insert(0,  'date', date)
    df_output_centrality.insert(0, 'date', date)
    
    
    # 解析モデルのデバッグ情報の出力
    tmp_path = 'daily/' + date + '_debug_data/'
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_debug_inte, 'var_model_intercept',   need_index=True)
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_debug_coef, 'var_model_coefficient', need_index=True)
    
    # 解析結果の出力
    tmp_path = 'daily/'
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_output_causality,  'causality',  need_index=False)
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_output_centrality, 'centrality', need_index=False)
    
    
    
    # 人気エリアのみOUTPUTへ出力
    tmp_path = f'人気エリア/daily/'
    use_ul.write_popular_area('ALL', '', 'daily', tmp_path, df_output_centrality, '人気エリア')
    
    
    
    # ====================================================================
    # 複数回遊の影響度解析処理・およびその出力処理
    # ====================================================================
    
    # 5階層以上には対応していないため、4階層以上の場合はエラーを出す
    if   migration_num >  4:
        raise ValueError(f"5階層以上の解析には対応していません。migration_num={migration_num}")
    
    # 4階層の場合には、3・4階層の解析を行う
    elif migration_num == 4:
        df_migrate_two = df_output_causality
        df_migrate_three, df_migrate_fourth = get_migration(df_migrate_two)
        
        tmp_path = 'daily/'
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_two,    'migration_2', need_index=False)
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_three,  'migration_3', need_index=False)
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_fourth, 'migration_4', need_index=False)
    
    # 3階層の場合には、3階層の解析を行う
    elif migration_num == 3:
        df_migrate_two   = df_output_causality
        df_migrate_three = get_migration_three(df_migrate_two)
        
        tmp_path = 'daily/'
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_two,   'migration_2', need_index=False)
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_three, 'migration_3', need_index=False)
    
    # 2階層の場合には、2階層の解析を行う
    elif migration_num == 2:
        df_migrate_two = df_output_causality
        
        tmp_path = 'daily/'
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_two, 'migration_2', need_index=False)
    
    else:
        raise ValueError(f"2階層未満の解析には対応していません。migration_num={migration_num}")
    
    
    return None

'''
日毎のネットワークモデル解析
人気エリア(Popular area)： 全フロア
影響量(Causal quantity)： フロア別
'''
def nwm_daily_pALL_cSEPARATE(calc_model:time_series_model, use_dl:download_to_file, use_ul:upload_to_file, param_dict:dict) -> None:
    
    date             = param_dict['date']                               # 解析対象日
    part_id          = param_dict['part_id']                            # 解析対象のパートID
    ueid_list        = param_dict['part_to_unique_list']                # 解析対象のユニークIDリスト
    sn_id_dict       = param_dict['section_to_unique_dict']             # セクション単位のユニークID辞書
    sn_ssn_id_dict   = param_dict['subsection_to_unique_dict']          # サブセクション単位のユニークID辞書
    start_time       = param_dict['daily_start_time']                   # 日別開始時刻
    end_time         = param_dict['daily_end_time']                     # 日別終了時刻
    time_interval    = param_dict['daily_time_interval']                # 日別時間間隔
    migration_num    = param_dict['daily_estimate_migration_number'][1] # 必要回遊階層数
    flg_group        = param_dict['GROUP_MODE_FLAG']                    # グループモードフラグ
    flg_specify_time = param_dict['SPECIFY_TIME_FLAG']                  # 時間指定フラグ
    

    
    # ====================================================================
    # 解析対象時系列データ取得処理
    # ====================================================================
    
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ準備時間 計測開始")
    
    # 1分間隔時系列データの取得
    df = get_time_series_data(use_dl, date, ueid_list)
    
    # 時間指定が有効な場合
    if flg_specify_time:
        # start_time ~ end_time までのデータを抽出
        df = extract_data_by_time(df, date, start_time, end_time)
    
    # 指定時間隔計測に置き換える
    # [15:00, 15:05) labeled 15:00 → setting(rule='5min, closed='left', label='left')
    df_granul = df.set_index(keys='datetime').resample(rule=time_interval, axis=0, closed='left', label='left').sum()
    
    # グループモードが有効な場合
    if flg_group:
        # サブセクション毎にユニークIDを一まとめにする
        
        # サブセクション単位のユニークID辞書を取得
        ssn_id_dict = get_subsection_to_unique_dict(sn_ssn_id_dict)
        
        # サブセクション単位でデータを整形
        for subsection_id in ssn_id_dict.keys():
            # ユニークIDリストの取得
            unique_id_list = ssn_id_dict[subsection_id]
            
            # グループ単位に置き換える
            df_granul[subsection_id] = df_granul[unique_id_list].sum(axis='columns')
        
        # ユニークIDリストを削除
        df_granul = df_granul.drop(columns=ueid_list)
    
    # 時間計測完了
    time_end  = time.perf_counter()
    time_diff = time_end - time_start
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ準備時間 {time_diff}秒")
    
    # 解析対象の時系列データの出力
    tmp_path = 'daily/' + date + '_debug_data/'
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_granul, 'input_time_series_data', need_index=True)
    
    # ====================================================================
    # 解析対象時系列データ解析処理
    # ====================================================================
    
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ解析(人気エリア)時間 計測開始")
    
    # 1日単位ごとにNWM計算を行う
    df_output_centrality, (df_debug_inte, df_debug_coef) = get_popularity(calc_model, df_granul)
    
    # 時間計測完了
    time_end = time.perf_counter()
    time_diff  = time_end - time_start
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ解析(人気エリア)時間 {time_diff}秒")
    
    # ====================================================================
    # 解析対象時系列データ解析結果 出力処理
    # ====================================================================
    
    # データフレームの先頭にdateを追記する
    df_output_centrality.insert(0, 'date', date)
    
    
    # 解析モデルのデバッグ情報の出力
    tmp_path = 'daily/' + date + '_debug_data/'
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_debug_inte, 'var_model_intercept',   need_index=True)
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_debug_coef, 'var_model_coefficient', need_index=True)
    
    # 解析結果の出力
    tmp_path = 'daily/'
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_output_centrality, 'centrality', need_index=False)
    
    
    # 人気エリアのみOUTPUTへ出力
    tmp_path = '人気エリア/daily/'
    use_ul.write_popular_area('ALL', '', 'daily', tmp_path, df_output_centrality, '人気エリア')
    
    
    
    # ====================================================================
    # 複数回遊の影響度解析処理・およびその出力処理
    # ====================================================================
    
    # セクションIDごとの因果量を解析する
    for idx, section_id in enumerate(sn_id_dict.keys()):
        # ユニークIDリストの取得
        ueid_list = sn_id_dict[section_id]
        
        # ====================================================================
        # 解析対象時系列データ取得処理
        # ====================================================================
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ準備時間 計測開始")
    
        # 1分間隔時系列データの取得
        df = get_time_series_data(use_dl, date, ueid_list)
        
        # 時間指定が有効な場合
        if flg_specify_time:
            # start_time ~ end_time までのデータを抽出
            df = extract_data_by_time(df, date, start_time, end_time)
            
    
        # 指定時間隔計測に置き換える
        # [15:00, 15:05) labeled 15:00 → setting(rule='5min, closed='left', label='left')
        df_granul = df.set_index(keys='datetime').resample(rule=time_interval, axis=0, closed='left', label='left').sum()
        
        # グループモードが有効な場合
        if flg_group:
            # サブセクション毎にユニークIDを一まとめにする
            
            # サブセクション単位のユニークID辞書を取得
            ssn_id_dict = sn_ssn_id_dict[section_id]
            
            # サブセクション単位でデータを整形
            for subsection_id in ssn_id_dict.keys():
                # ユニークIDリストの取得
                unique_id_list = ssn_id_dict[subsection_id]
                
                # グループ単位に置き換える
                df_granul[subsection_id] = df_granul[unique_id_list].sum(axis='columns')
            
            # ユニークIDリストを削除
            df_granul = df_granul.drop(columns=ueid_list)
    
        # 時間計測完了
        time_end  = time.perf_counter()
        time_diff = time_end - time_start
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ準備時間 {time_diff}秒")
        
        # 解析対象の時系列データの出力
        tmp_path = 'daily/' + date + '_debug_data/'
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_granul, 'input_time_series_data', need_index=True)
        
        # ====================================================================
        # 解析対象時系列データ解析処理
        # ====================================================================
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ解析(因果量)時間 計測開始")
        
        # 1日単位ごとにNWM計算を行う
        df_output_causality, (df_debug_inte, df_debug_coef) = get_causality(calc_model, df_granul)
        
        # 時間計測完了
        time_end = time.perf_counter()
        time_diff  = time_end - time_start
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ解析(因果量)時間 {time_diff}秒")
        
        # ====================================================================
        # 解析対象時系列データ解析結果 出力処理
        # ====================================================================
        
        # データフレームの先頭にdateを追記する
        df_output_causality.insert(0,  'date', date)
    
        # 解析モデルのデバッグ情報の出力
        tmp_path = 'daily/' + date + '_debug_data/'
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_debug_inte, 'var_model_intercept',   need_index=True)
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_debug_coef, 'var_model_coefficient', need_index=True)
        
        # 解析結果の出力
        tmp_path = 'daily/'
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_output_causality, 'causality', need_index=False)
        
        
        
        # ====================================================================
        # 複数回遊の影響度解析処理・およびその出力処理
        # ====================================================================
        
        # 5階層以上には対応していないため、4階層以上の場合はエラーを出す
        if   migration_num >  4:
            raise ValueError(f"5階層以上の解析には対応していません。migration_num={migration_num}")
        
        # 4階層の場合には、3・4階層の解析を行う
        elif migration_num == 4:
            df_migrate_two = df_output_causality
            df_migrate_three, df_migrate_fourth = get_migration(df_migrate_two)
            
            tmp_path = 'daily/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_two,    'migration_2', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_three,  'migration_3', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_fourth, 'migration_4', need_index=False)
        
        # 3階層の場合には、3階層の解析を行う
        elif migration_num == 3:
            df_migrate_two   = df_output_causality
            df_migrate_three = get_migration_three(df_migrate_two)
            
            tmp_path = 'daily/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_two,   'migration_2', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_three, 'migration_3', need_index=False)
        
        # 2階層の場合には、2階層の解析を行う
        elif migration_num == 2:
            df_migrate_two = df_output_causality
            
            tmp_path = 'daily/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_two, 'migrate_2', need_index=False)
        
        else:
            raise ValueError(f"2階層未満の解析には対応していません。migration_num={migration_num}")
    
    
    return None

'''
日毎のネットワークモデル解析
人気エリア(Popular area)： フロア別
影響量(Causal quantity)： フロア別
'''
def nwm_daily_pSEPARATE_cSEPARATE(calc_model:time_series_model, use_dl:download_to_file, use_ul:upload_to_file, param_dict:dict) -> None:
    
    date             = param_dict['date']                               # 解析対象日
    part_id          = param_dict['part_id']                            # 解析対象のパートID
    ueid_list        = param_dict['part_to_unique_list']                # 解析対象のユニークIDリスト
    sn_id_dict       = param_dict['section_to_unique_dict']             # セクション単位のユニークID辞書
    sn_ssn_id_dict   = param_dict['subsection_to_unique_dict']          # サブセクション単位のユニークID辞書
    start_time       = param_dict['daily_start_time']                   # 日別開始時刻
    end_time         = param_dict['daily_end_time']                     # 日別終了時刻
    time_interval    = param_dict['daily_time_interval']                # 日別時間間隔
    migration_num    = param_dict['daily_estimate_migration_number'][1] # 必要回遊階層数
    flg_group        = param_dict['GROUP_MODE_FLAG']                    # グループモードフラグ
    flg_specify_time = param_dict['SPECIFY_TIME_FLAG']                  # 時間指定フラグ
    
    
    
    # セクションIDごとにデータを解析する
    for idx, section_id in enumerate(sn_id_dict.keys()):
        # ユニークIDリストの取得
        ueid_list = sn_id_dict[section_id]
        
        # ====================================================================
        # 解析対象時系列データ取得処理
        # ====================================================================
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ準備時間 計測開始")
    
        # 1分間隔時系列データの取得
        df = get_time_series_data(use_dl, date, ueid_list)
        
        # 時間指定が有効な場合
        if flg_specify_time:
            # start_time ~ end_time までのデータを抽出
            df = extract_data_by_time(df, date, start_time, end_time)

        # 指定時間隔計測に置き換える
        # [15:00, 15:05) labeled 15:00 → setting(rule='5min, closed='left', label='left')
        df_granul = df.set_index(keys='datetime').resample(rule=time_interval, axis=0, closed='left', label='left').sum()
        
        # グループモードが有効な場合
        if flg_group:
            # サブセクション単位のユニークID辞書を取得
            ssn_id_dict = sn_ssn_id_dict[section_id]
            
            # サブセクション単位でデータを整形
            for subsection_id in ssn_id_dict.keys():
                # ユニークIDリストの取得
                unique_id_list = ssn_id_dict[subsection_id]
                
                # グループ単位に置き換える
                df_granul[subsection_id] = df_granul[unique_id_list].sum(axis='columns')
            
            # ユニークIDリストを削除
            df_granul = df_granul.drop(columns=ueid_list)
    
        # 時間計測完了
        time_end  = time.perf_counter()
        time_diff = time_end - time_start
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ準備時間 {time_diff}秒")
        
        # 解析対象の時系列データの出力
        tmp_path = 'daily/' + date + '_debug_data/'
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_granul, 'input_time_series_data', need_index=True)
        
        # ====================================================================
        # 解析対象時系列データ解析処理
        # ====================================================================
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ解析(人気エリア・因果量)時間 計測開始")

        # 1日単位ごとにNWM計算を行う
        df_output_causality, df_output_centrality, (df_debug_inte, df_debug_coef) = get_influence(calc_model, df_granul)
        
        # 時間計測完了
        time_end = time.perf_counter()
        time_diff  = time_end - time_start
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ解析(人気エリア・因果量)時間 {time_diff}秒")
        
        # ====================================================================
        # 解析対象時系列データ解析結果 出力処理
        # ====================================================================
        
        # データフレームの先頭にdateを追記する
        df_output_causality.insert(0,  'date', date)
        df_output_centrality.insert(0, 'date', date)
        

        # 解析モデルのデバッグ情報の出力
        tmp_path = 'daily/' + date + '_debug_data/'
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_debug_inte, 'var_model_intercept',   need_index=True)
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_debug_coef, 'var_model_coefficient', need_index=True)
        
        # 解析結果の出力            
        tmp_path = 'daily/'
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_output_causality,  'causality',  need_index=False)
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_output_centrality, 'centrality', need_index=False)
        
        
        # 人気エリアのみOUTPUTへ出力
        tmp_path = '人気エリア/daily/'
        use_ul.write_popular_area('SEPARATE', section_id, 'daily', tmp_path, df_output_centrality, '人気エリア')
        
        
        
        # ====================================================================
        # 複数回遊の影響度解析処理・およびその出力処理
        # ====================================================================
        
        # 5階層以上には対応していないため、4階層以上の場合はエラーを出す
        if   migration_num >  4:
            raise ValueError(f"5階層以上の解析には対応していません。migration_num={migration_num}")
        
        # 4階層の場合には、3・4階層の解析を行う
        elif migration_num == 4:
            df_migrate_two = df_output_causality
            df_migrate_three, df_migrate_fourth = get_migration(df_migrate_two)
            
            tmp_path = 'daily/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, df_migrate_two,    'migration_2', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, df_migrate_three,  'migration_3', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, df_migrate_fourth, 'migration_4', need_index=False)
        
        # 3階層の場合には、3階層の解析を行う
        elif migration_num == 3:
            df_migrate_two   = df_output_causality
            df_migrate_three = get_migration_three(df_migrate_two)
            
            tmp_path = 'daily/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_two,   'migration_2', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_three, 'migration_3', need_index=False)
        
        # 2階層の場合には、2階層の解析を行う
        elif migration_num == 2:
            df_migrate_two = df_output_causality
            
            tmp_path = 'daily/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_two, 'migration_2', need_index=False)
        
        else:
            raise ValueError(f"2階層未満の解析には対応していません。migration_num={migration_num}")
            
    
    return None


'''
時間毎のネットワークモデル解析
人気エリア(Popular area)： 全フロア
影響量(Causal quantity)： 全フロア
'''
def nwm_hourly_pALL_cALL(calc_model:time_series_model, use_dl:download_to_file, use_ul:upload_to_file, param_dict:dict) -> None:
    
    date             = param_dict['date']                                # 解析対象日
    part_id          = param_dict['part_id']                             # 解析対象のパートID
    ueid_list        = param_dict['part_to_unique_list']                 # 解析対象のユニークIDリスト
    sn_id_dict       = param_dict['section_to_unique_dict']              # セクション単位のユニークID辞書
    sn_ssn_id_dict   = param_dict['subsection_to_unique_dict']           # サブセクション単位のユニークID辞書
    start_time       = param_dict['hourly_start_time']                   # 時間別開始時刻
    end_time         = param_dict['hourly_end_time']                     # 時間別終了時刻
    time_interval    = param_dict['hourly_time_interval']                # 時間別時間間隔
    migration_num    = param_dict['hourly_estimate_migration_number'][1] # 必要回遊階層数
    flg_group        = param_dict['GROUP_MODE_FLAG']                     # グループモードフラグ
    flg_specify_time = param_dict['SPECIFY_TIME_FLAG']                   # 時間指定フラグ
    
    
    
    # ====================================================================
    # 解析対象時系列データ取得処理
    # ====================================================================
    
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ準備 計測開始")
    
    # 1分間隔時系列データの取得
    df = get_time_series_data(use_dl, date, ueid_list)
    
    # 時間指定が有効な場合
    if flg_specify_time:
        # start_time ~ end_time までのデータを抽出
        df = extract_data_by_time(df, date, start_time, end_time)
    
    # datetimeからhourをとりだし、指定時間隔計測に置き換える
    # [15:00, 15:05) labeled 15:00 → setting(rule='5min, closed='left', label='left')
    df['hour'] = df['datetime'].apply(lambda x: int(x.strftime('%H')))
    df_granul  = df.set_index(keys='datetime').resample(rule=time_interval, axis=0, closed='left', label='left').sum()
    
    # グループモードが有効な場合
    if flg_group:
        # サブセクション単位のユニークID辞書を取得
        ssn_id_dict = get_subsection_to_unique_dict(sn_ssn_id_dict)
        
        # サブセクション単位でデータを整形
        for subsection_id in ssn_id_dict.keys():
            # ユニークIDリストの取得
            unique_id_list = ssn_id_dict[subsection_id]
            
            # グループ単位に置き換える
            df_granul[subsection_id] = df_granul[unique_id_list].sum(axis='columns')
        
        # ユニークIDリストを削除
        df_granul = df_granul.drop(columns=ueid_list)
    
    # 時間計測完了
    time_end  = time.perf_counter()
    time_diff = time_end - time_start
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ準備時間 {time_diff}秒")
    
    # 解析対象の時系列データの出力
    tmp_path = 'hourly/' + date + '_debug_data/'
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_granul, 'input_time_series_data', need_index=True)
    
    # ====================================================================
    # 解析対象時系列データ解析処理
    # ====================================================================
    
    # 空のデータフレームの作成
    list_output_causality  = []
    list_output_centrality = []
    # 1時間毎に処理を行う
    for h in df_granul['hour'].drop_duplicates().values.tolist():
        df_hour = df_granul[df_granul['hour'] == h]
        df_hour = df_hour.drop('hour', axis=1)
        
        # 解析対象の時系列データの出力
        tmp_path = 'hourly/' + date + '_debug_data/'
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_hour, f'{str(h).zfill(2)}:00' + '_input_time_series_data', need_index=True)
        
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date} {str(h).zfill(2)}:00:00:: Part_ID:{part_id}  {time_interval}間隔時系列データ解析(人気エリア・因果量)時間 計測開始")
        
        # 1日単位ごとにNWM計算を行う
        df_output_causality, df_output_centrality, (df_debug_inte, df_debug_coef) = get_influence(calc_model, df_hour)
        
        # データフレームの先頭にdate, hourを追記する
        df_output_causality.insert(0,  'date', f'{date} {str(h).zfill(2)}:00:00')
        df_output_centrality.insert(0, 'date', date)
        df_output_centrality.insert(1, 'hour', h)
        
        
        # 時間計測完了
        time_end = time.perf_counter()
        time_diff  = time_end - time_start
        print(f"{date} {str(h).zfill(2)}:00:00:: Part_ID:{part_id}  {time_interval}間隔時系列データ解析(人気エリア・因果量)時間 {time_diff}秒")
        
        # データフレームの登録
        list_output_causality.append(df_output_causality)
        list_output_centrality.append(df_output_centrality)
        
        # 解析モデルのデバッグ情報の出力
        tmp_path = 'hourly/' + date + '_debug_data/'
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_debug_inte, f'{str(h).zfill(2)}:00' + '_var_model_intercept',   need_index=True)
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_debug_coef, f'{str(h).zfill(2)}:00' + '_var_model_coefficient', need_index=True)
        
    # ====================================================================
    # 解析対象時系列データ解析結果 出力処理
    # ====================================================================
    
    # 縦結合する
    df_output_causality  = pd.concat(list_output_causality,  axis=0).sort_values('date', ignore_index=True)
    df_output_centrality = pd.concat(list_output_centrality, axis=0).sort_values('date', ignore_index=True)
    
    # 解析結果の出力
    tmp_path = 'hourly/'
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_output_causality,  'causality',  need_index=False)
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_output_centrality, 'centrality', need_index=False)
    
    # 人気エリアのみOUTPUTへ出力
    tmp_path = '人気エリア/hourly/'
    use_ul.write_popular_area('ALL', '', 'hourly', tmp_path, df_output_centrality, '人気エリア')
    
    
    # ====================================================================
    # 複数回遊の影響度解析処理・およびその出力処理
    # ====================================================================
    
    # 5階層以上には対応していないため、4階層以上の場合はエラーを出す
    if   migration_num >  4:
        raise ValueError(f"5階層以上の解析には対応していません。migration_num={migration_num}")
    
    # 4階層の場合には、3・4階層の解析を行う
    elif migration_num == 4:
        df_migrate_two = df_output_causality
        df_migrate_three, df_migrate_fourth = get_migration(df_migrate_two)
        
        tmp_path = 'hourly/'
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_two,    'migration_2', need_index=False)
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_three,  'migration_3', need_index=False)
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_fourth, 'migration_4', need_index=False)
    
    # 3階層の場合には、3階層の解析を行う
    elif migration_num == 3:
        df_migrate_two   = df_output_causality
        df_migrate_three = get_migration_three(df_migrate_two)
        
        tmp_path = 'hourly/'
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_two,    'migration_2', need_index=False)
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_three,  'migration_3', need_index=False)
        
    # 2階層の場合には、2階層の解析を行う
    elif migration_num == 2:
        df_migrate_two = df_output_causality
        
        tmp_path = 'hourly/'
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_migrate_two,    'migration_2', need_index=False)
    
    else:
        raise ValueError(f"2階層未満の解析には対応していません。migration_num={migration_num}")
    
    return None

'''
時間毎のネットワークモデル解析
人気エリア(Popular area)： 全フロア
影響量(Causal quantity)： フロア別
'''
def nwm_hourly_pALL_cSEPARATE(calc_model:time_series_model, use_dl:download_to_file, use_ul:upload_to_file, param_dict:dict) -> None:
        
    date             = param_dict['date']                                # 解析対象日
    part_id          = param_dict['part_id']                             # 解析対象のパートID
    ueid_list        = param_dict['part_to_unique_list']                 # 解析対象のユニークIDリスト
    sn_id_dict       = param_dict['section_to_unique_dict']              # セクション単位のユニークID辞書
    sn_ssn_id_dict   = param_dict['subsection_to_unique_dict']           # サブセクション単位のユニークID辞書
    start_time       = param_dict['hourly_start_time']                   # 時間別開始時刻
    end_time         = param_dict['hourly_end_time']                     # 時間別終了時刻
    time_interval    = param_dict['hourly_time_interval']                # 時間別時間間隔
    migration_num    = param_dict['hourly_estimate_migration_number'][1] # 必要回遊階層数
    flg_group        = param_dict['GROUP_MODE_FLAG']                     # グループモードフラグ
    flg_specify_time = param_dict['SPECIFY_TIME_FLAG']                   # 時間指定フラグ
    
    
    
    # ====================================================================
    # 解析対象時系列データ取得処理
    # ====================================================================
    
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ準備 計測開始")
    
    # 1分間隔時系列データの取得
    df = get_time_series_data(use_dl, date, ueid_list)
    
    # 時間指定が有効な場合
    if flg_specify_time:
        # start_time ~ end_time までのデータを抽出
        df = extract_data_by_time(df, date, start_time, end_time)
    
    # datetimeからhourをとりだし、指定時間隔計測に置き換える
    # [15:00, 15:05) labeled 15:00 → setting(rule='5min, closed='left', label='left')
    df['hour'] = df['datetime'].apply(lambda x: int(x.strftime('%H')))
    df_granul  = df.set_index(keys='datetime').resample(rule=time_interval, axis=0, closed='left', label='left').sum()
    
    # グループモードが有効な場合
    if flg_group:
        # サブセクション単位のユニークID辞書を取得
        ssn_id_dict = get_subsection_to_unique_dict(sn_ssn_id_dict)
        
        # サブセクション単位でデータを整形
        for subsection_id in ssn_id_dict.keys():
            # ユニークIDリストの取得
            unique_id_list = ssn_id_dict[subsection_id]
            
            # グループ単位に置き換える
            df_granul[subsection_id] = df_granul[unique_id_list].sum(axis='columns')
        
        # ユニークIDリストを削除
        df_granul = df_granul.drop(columns=ueid_list)
    
    # 時間計測完了
    time_end  = time.perf_counter()
    time_diff = time_end - time_start
    print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ準備時間 {time_diff}秒")
    
    # 解析対象の時系列データの出力
    tmp_path = 'hourly/' + date + '_debug_data/'
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_granul, 'input_time_series_data', need_index=True)
    
    # ====================================================================
    # 解析対象時系列データ解析処理
    # ====================================================================
    
    # 空のデータフレームの作成
    list_output_centrality = []
    # 1時間毎に処理を行う
    for h in df_granul['hour'].drop_duplicates().values.tolist():
        df_hour = df_granul[df_granul['hour'] == h]
        df_hour = df_hour.drop('hour', axis=1)
        
        # 解析対象の時系列データの出力
        tmp_path = 'hourly/' + date + '_debug_data/'
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_hour, f'{str(h).zfill(2)}:00' + '_input_time_series_data', need_index=True)
        
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date} {str(h).zfill(2)}:00:00:: Part_ID:{part_id}  {time_interval}間隔時系列データ解析(人気エリア)時間 計測開始")
        
        # 1日単位ごとにNWM計算を行う
        df_output_centrality, (df_debug_inte, df_debug_coef) = get_popularity(calc_model, df_hour)
        
        # データフレームの先頭にdateを追記する
        df_output_centrality.insert(0, 'date', date)
        df_output_centrality.insert(1, 'hour', h)
        
        
        # 時間計測完了
        time_end = time.perf_counter()
        time_diff  = time_end - time_start
        print(f"{date} {str(h).zfill(2)}:00:00:: Part_ID:{part_id}  {time_interval}間隔時系列データ解析(人気エリア)時間 {time_diff}秒")
        
        # データフレームの登録
        list_output_centrality.append(df_output_centrality)
        
        # 解析モデルのデバッグ情報の出力
        tmp_path = 'hourly/' + date + '_debug_data/'
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_debug_inte, f'{str(h).zfill(2)}:00' + '_var_model_intercept',   need_index=True)
        use_ul.debug_write_csv_file('ALL', '', tmp_path, df_debug_coef, f'{str(h).zfill(2)}:00' + '_var_model_coefficient', need_index=True)
    
    # ====================================================================
    # 解析対象時系列データ解析結果 出力処理
    # ====================================================================
    
    # 縦結合する
    df_output_centrality = pd.concat(list_output_centrality, axis=0).sort_values('date', ignore_index=True)
    
    # 解析結果の出力
    tmp_path = 'hourly/'
    use_ul.debug_write_csv_file('ALL', '', tmp_path, df_output_centrality, 'centrality', need_index=False)
    
    # 人気エリアのみOUTPUTへ出力
    tmp_path = '人気エリア/hourly/'
    use_ul.write_popular_area('ALL', '', 'hourly', tmp_path, df_output_centrality, '人気エリア')
    
    
    
    # ====================================================================
    # 複数回遊の影響度解析処理・およびその出力処理
    # ====================================================================
    

    for idx, section_id in enumerate(sn_id_dict.keys()):
        # ユニークIDリストの取得
        ueid_list = sn_id_dict[section_id]
        
        # ====================================================================
        # 解析対象時系列データ取得処理
        # ====================================================================
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ準備時間 計測開始")

        # 1分間隔時系列データの取得
        df = get_time_series_data(use_dl, date, ueid_list)
        
        # 時間指定が有効な場合
        if flg_specify_time:
            # start_time ~ end_time までのデータを抽出
            df = extract_data_by_time(df, date, start_time, end_time)
        
        # datetimeからhourをとりだし、指定時間隔計測に置き換える
        # [15:00, 15:05) labeled 15:00 → setting(rule='5min, closed='left', label='left')
        df['hour'] = df['datetime'].apply(lambda x: int(x.strftime('%H')))
        df_granul  = df.set_index(keys='datetime').resample(rule=time_interval, axis=0, closed='left', label='left').sum()
        
        # グループモードが有効な場合
        if flg_group:
            # サブセクション単位のユニークID辞書を取得
            ssn_id_dict = sn_ssn_id_dict[section_id]
            
            # サブセクション単位でデータを整形
            for subsection_id in ssn_id_dict.keys():
                # ユニークIDリストの取得
                unique_id_list = ssn_id_dict[subsection_id]
                
                # グループ単位に置き換える
                df_granul[subsection_id] = df_granul[unique_id_list].sum(axis='columns')
            
            # ユニークIDリストを削除
            df_granul = df_granul.drop(columns=ueid_list)
    
        # 時間計測完了
        time_end  = time.perf_counter()
        time_diff = time_end - time_start
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ準備時間 {time_diff}秒")
        
        # 解析対象の時系列データの出力
        tmp_path = 'hourly/' + date + '_debug_data/'
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_granul, 'input_time_series_data', need_index=True)
        
        # ====================================================================
        # 解析対象時系列データ解析処理
        # ====================================================================

        # 空のデータフレームの作成
        list_output_causality  = []
        # 1時間毎に処理を行う
        for h in df_granul['hour'].drop_duplicates().values.tolist():
            df_hour = df_granul[df_granul['hour'] == h]
            df_hour = df_hour.drop('hour', axis=1)
            
            # 解析対象の時系列データの出力
            tmp_path = 'hourly/' + date + '_debug_data/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_hour, f'{str(h).zfill(2)}:00' + '_input_time_series_data', need_index=True)
            
            
            # 時間計測開始
            time_start = time.perf_counter()
            print(f"{date} {str(h).zfill(2)}:00:00:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ解析(因果量)時間 計測開始")

            # 1日単位ごとにNWM計算を行う
            df_output_causality, (df_debug_inte, df_debug_coef) = get_causality(calc_model, df_hour)
            
            # データフレームの先頭にdateを追記する
            df_output_causality.insert(0,  'date', f'{date} {str(h).zfill(2)}:00:00')

            # 時間計測完了
            time_end = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"{date} {str(h).zfill(2)}:00:00:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ解析(因果量)時間 {time_diff}秒")
            
            # データフレームの登録
            list_output_causality.append(df_output_causality)

            # 解析モデルのデバッグ情報の出力
            tmp_path = 'hourly/' + date + '_debug_data/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_debug_inte, f'{str(h).zfill(2)}:00' + '_var_model_intercept',   need_index=True)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_debug_coef, f'{str(h).zfill(2)}:00' + '_var_model_coefficient', need_index=True)
        
        # ====================================================================
        # 解析対象時系列データ解析結果 出力処理
        # ====================================================================
        
        # 縦結合する
        df_output_causality = pd.concat(list_output_causality,  axis=0).sort_values('date', ignore_index=True)
        
        # 解析結果の出力
        tmp_path = 'hourly/'
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_output_causality, 'causality',  need_index=False)

        
        
        # ====================================================================
        # 複数回遊の影響度解析処理・およびその出力処理
        # ====================================================================
        
        # 5階層以上には対応していないため、4階層以上の場合はエラーを出す
        if   migration_num >  4:
            raise ValueError(f"5階層以上の解析には対応していません。migration_num={migration_num}")
        
        # 4階層の場合には、3・4階層の解析を行う
        elif migration_num == 4:
            df_migrate_two = df_output_causality
            df_migrate_three, df_migrate_fourth = get_migration(df_migrate_two)
            
            tmp_path = 'hourly/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_two,    'migration_2', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_three,  'migration_3', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_fourth, 'migration_4', need_index=False)
        
        # 3階層の場合には、3階層の解析を行う
        elif migration_num == 3:
            df_migrate_two   = df_output_causality
            df_migrate_three = get_migration_three(df_migrate_two)
            
            tmp_path = 'hourly/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_two,    'migration_2', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_three,  'migration_3', need_index=False)
        
        # 2階層の場合には、2階層の解析を行う
        elif migration_num == 2:
            df_migrate_two = df_output_causality
            
            tmp_path = 'hourly/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_two,    'migration_2', need_index=False)
        
        else:
            raise ValueError(f"2階層未満の解析には対応していません。migration_num={migration_num}")
    

    return None

'''
時間毎のネットワークモデル解析
人気エリア(Popular area)： フロア別
影響量(Causal quantity)： フロア別
'''
def nwm_hourly_pSEPARATE_cSEPARATE(calc_model:time_series_model, use_dl:download_to_file, use_ul:upload_to_file, param_dict:dict) -> None:
        
    date             = param_dict['date']                                # 解析対象日
    part_id          = param_dict['part_id']                             # 解析対象のパートID
    sn_id_dict       = param_dict['section_to_unique_dict']              # セクション単位のユニークID辞書
    sn_ssn_id_dict   = param_dict['subsection_to_unique_dict']           # サブセクション単位のユニークID辞書
    start_time       = param_dict['hourly_start_time']                   # 時間別開始時刻
    end_time         = param_dict['hourly_end_time']                     # 時間別終了時刻
    time_interval    = param_dict['hourly_time_interval']                # 時間別時間間隔
    migration_num    = param_dict['hourly_estimate_migration_number'][1] # 必要回遊階層数
    flg_group        = param_dict['GROUP_MODE_FLAG']                     # グループモードフラグ
    flg_specify_time = param_dict['SPECIFY_TIME_FLAG']                   # 時間指定フラグ
    
    
    
    for idx, section_id in enumerate(sn_id_dict.keys()):
        # ユニークIDリストの取得
        ueid_list = sn_id_dict[section_id]
        
        # ====================================================================
        # 解析対象時系列データ取得処理
        # ====================================================================
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ準備 計測開始")

        # 1分間隔時系列データの取得
        df = get_time_series_data(use_dl, date, ueid_list)
        
        # 時間指定が有効な場合
        if flg_specify_time:
            # start_time ~ end_time までのデータを抽出
            df = extract_data_by_time(df, date, start_time, end_time)
        
        # datetimeからhourをとりだし、指定時間隔計測に置き換える
        # [15:00, 15:05) labeled 15:00 → setting(rule='5min, closed='left', label='left')
        df['hour'] = df['datetime'].apply(lambda x: int(x.strftime('%H')))
        df_granul  = df.set_index(keys='datetime').resample(rule=time_interval, axis=0, closed='left', label='left').sum()
        
        # グループモードが有効な場合
        if flg_group:
            # サブセクション単位のユニークID辞書を取得
            ssn_id_dict = sn_ssn_id_dict[section_id]
            
            # サブセクション単位でデータを整形
            for subsection_id in ssn_id_dict.keys():
                # ユニークIDリストの取得
                unique_id_list = ssn_id_dict[subsection_id]
                
                # グループ単位に置き換える
                df_granul[subsection_id] = df_granul[unique_id_list].sum(axis='columns')
            
            # ユニークIDリストを削除
            df_granul = df_granul.drop(columns=ueid_list)
    
        # 時間計測完了
        time_end  = time.perf_counter()
        time_diff = time_end - time_start
        print(f"{date}:: Part_ID:{part_id}  {time_interval}間隔時系列データ準備時間 {time_diff}秒")
        
        # 解析対象の時系列データの出力
        tmp_path = 'hourly/' + date + '_debug_data/'
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_granul, 'input_time_series_data', need_index=True)
        
        # ====================================================================
        # 解析対象時系列データ解析処理
        # ====================================================================

        # 空のデータフレームの作成
        list_output_causality  = []
        list_output_centrality = []
        # 1時間毎に処理を行う
        for h in df_granul['hour'].drop_duplicates().values.tolist():
            df_hour = df_granul[df_granul['hour'] == h]
            df_hour = df_hour.drop('hour', axis=1)
            
            # 解析対象の時系列データの出力
            tmp_path = 'hourly/' + date + '_debug_data/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_hour, f'{str(h).zfill(2)}:00' + '_input_time_series_data', need_index=True)
            
            
            # 時間計測開始
            time_start = time.perf_counter()
            print(f"{date} {str(h).zfill(2)}:00:00:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ解析(人気エリア・因果量)時間 計測開始")
            
            # 1日単位ごとにNWM計算を行う
            df_output_causality, df_output_centrality, (df_debug_inte, df_debug_coef) = get_influence(calc_model, df_hour)
            
            # データフレームの先頭にdateを追記する
            df_output_causality.insert(0,  'date', f'{date} {str(h).zfill(2)}:00:00')
            df_output_centrality.insert(0, 'date', date)
            df_output_centrality.insert(1, 'hour', h)
            

            # 時間計測完了
            time_end = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"{date} {str(h).zfill(2)}:00:00:: Part_ID:{part_id} Section-{idx+1}:{section_id}  {time_interval}間隔時系列データ解析(人気エリア・因果量)時間 {time_diff}秒")
            
            # データフレームの登録
            list_output_causality.append(df_output_causality)
            list_output_centrality.append(df_output_centrality)

            # 解析モデルのデバッグ情報の出力
            tmp_path = 'hourly/' + date + '_debug_data/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_debug_inte, f'{str(h).zfill(2)}:00' + '_var_model_intercept',   need_index=True)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_debug_coef, f'{str(h).zfill(2)}:00' + '_var_model_coefficient', need_index=True)
        
        # ====================================================================
        # 解析対象時系列データ解析結果 出力処理
        # ====================================================================
        
        # 縦結合する
        df_output_causality  = pd.concat(list_output_causality,  axis=0).sort_values('date', ignore_index=True)
        df_output_centrality = pd.concat(list_output_centrality, axis=0).sort_values('date', ignore_index=True)
        
        # 解析結果の出力
        tmp_path = 'hourly/'
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_output_causality,  'causality',  need_index=False)
        use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_output_centrality, 'centrality', need_index=False)
        
        # 人気エリアのみOUTPUTへ出力
        tmp_path = '人気エリア/hourly/'
        use_ul.write_popular_area('SEPARATE', section_id, 'hourly', tmp_path, df_output_centrality, '人気エリア')
        
        
        
        # ====================================================================
        # 複数回遊の影響度解析処理・およびその出力処理
        # ====================================================================
        
        # 5階層以上には対応していないため、4階層以上の場合はエラーを出す
        if   migration_num >  4:
            raise ValueError(f"5階層以上の解析には対応していません。migration_num={migration_num}")
        
        # 4階層の場合には、3・4階層の解析を行う
        elif migration_num == 4:
            df_migrate_two = df_output_causality
            df_migrate_three, df_migrate_fourth = get_migration(df_migrate_two)
            
            tmp_path = 'hourly/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_two,    'migration_2', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_three,  'migration_3', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_fourth, 'migration_4', need_index=False)
        
        # 3階層の場合には、3階層の解析を行う
        elif migration_num == 3:
            df_migrate_two   = df_output_causality
            df_migrate_three = get_migration_three(df_migrate_two)
            
            tmp_path = 'hourly/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_two,    'migration_2', need_index=False)
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_three,  'migration_3', need_index=False)
        
        # 2階層の場合には、2階層の解析を行う
        elif migration_num == 2:
            df_migrate_two = df_output_causality
            
            tmp_path = 'hourly/'
            use_ul.debug_write_csv_file('SEPARATE', section_id, tmp_path, df_migrate_two,    'migration_2', need_index=False)
        
        else:
            raise ValueError(f"2階層未満の解析には対応していません。migration_num={migration_num}")
            

    return None


def networkmodel(spec_comb:UseInterface, param_dict:dict) -> None:
    # param_dictに期待する構成
    # dict
    #  |-- date                             : date,
    #  |-- part_id                          : str,
    #  |-- part_to_unique_list              : list[str],
    #  |-- section_to_unique_dict           : dict[str, list[str]],
    #  |-- subsection_to_unique_dict        : dict[str, dict[str, list[str]]],
    #  |-- folder_name                      : str,
    #  |-- daily_start_time                 : str,
    #  |-- daily_end_time                   : str,
    #  |-- daily_time_interval              : str,
    #  |-- daily_population_area            : str,
    #  |-- daily_estimate_migration_number  : str,
    #  |-- hourly_start_time                : str,
    #  |-- hourly_end_time                  : str,
    #  |-- hourly_time_interval             : str,
    #  |-- hourly_population_area           : str,
    #  |-- hourly_estimate_migration_number : str,
    #  |-- GROUP_MODE_FLAG                  : bool,
    #  |-- SPECIFY_TIME_FLAG                : bool,
    
    
    # 時間計測開始
    time_all_start = time.perf_counter()
    
    
    #====================================================================
    # 1日単位nwm処理
    #====================================================================
    print(f'1日単位nwm処理 開始')
    
    if   param_dict['daily_population_area'] == 'ALL_FLOOR':
        if   param_dict['daily_estimate_migration_number'][0] == 'ALL_FLOOR':
            # 人気エリア：すべてのフロア
            # 推定回遊人数：すべてのフロア
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： すべてのフロア')
            print(f'推定回遊人数：すべてのフロア')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            nwm_daily_pALL_cALL(spec_comb.model, spec_comb.download, spec_comb.upload, param_dict)
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"daily 総実行時間: {time_diff}秒")
            
        elif param_dict['daily_estimate_migration_number'][0] == 'SEPARATE_FLOOR':
            # 人気エリア：すべてのフロア
            # 推定回遊人数：フロアごとに分ける場合
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： すべてのフロア')
            print(f'推定回遊人数：フロア別')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            nwm_daily_pALL_cSEPARATE(spec_comb.model, spec_comb.download, spec_comb.upload, param_dict)
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"daily 総実行時間: {time_diff}秒")
        
        else:
            # 人気エリア：すべてのフロア
            # 推定回遊人数：処理なし
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： すべてのフロア')
            print(f'推定回遊人数：処理なし')
            
            # 時間計測開始
            time_start = time.perf_counter()
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"daily 総実行時間: {time_diff}秒")
    
    
    elif param_dict['daily_population_area'] == 'SEPARATE_FLOOR':
        if   param_dict['daily_estimate_migration_number'][0] == 'ALL_FLOOR':
            # 人気エリア：フロアごとに分ける場合
            # 推定回遊人数：すべてのフロア
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： フロア別')
            print(f'推定回遊人数：すべてのフロア')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"daily 総実行時間: {time_diff}秒")
            
        elif param_dict['daily_estimate_migration_number'][0] == 'SEPARATE_FLOOR':
            # 人気エリア：フロアごとに分ける場合
            # 推定回遊人数：フロアごとに分ける場合
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： フロア別')
            print(f'推定回遊人数：フロア別')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            nwm_daily_pSEPARATE_cSEPARATE(spec_comb.model, spec_comb.download, spec_comb.upload, param_dict)
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"daily 総実行時間: {time_diff}秒")
        
        else:
            # 人気エリア：フロアごとに分ける場合
            # 推定回遊人数：処理なし
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： フロア別')
            print(f'推定回遊人数：処理なし')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"daily 総実行時間: {time_diff}秒")
    
    else:
        if  param_dict['daily_estimate_migration_number'][0] == 'ALL_FLOOR':
            # 人気エリア：処理なし
            # 推定回遊人数：すべてのフロア
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： 処理なし')
            print(f'推定回遊人数：すべてのフロア')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"daily 総実行時間: {time_diff}秒")
        elif param_dict['daily_estimate_migration_number'][0] == 'SEPARATE_FLOOR':
            # 人気エリア：処理なし
            # 推定回遊人数：フロアごとに分ける場合
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： 処理なし')
            print(f'推定回遊人数：フロア別')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"daily 総実行時間: {time_diff}秒")
        else:
            # 人気エリア：処理なし
            # 推定回遊人数：処理なし
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： 処理なし')
            print(f'推定回遊人数：処理なし')
    
    print(f'1日単位nwm処理 終了')
    
    
    #====================================================================
    # 1時間単位nwm処理
    #====================================================================
    print(f'1時間単位nwm処理 開始')
    
    if   param_dict['hourly_population_area'] == 'ALL_FLOOR':
        if   param_dict['hourly_estimate_migration_number'][0] == 'ALL_FLOOR':
            # 人気エリア：すべてのフロア
            # 推定回遊人数：すべてのフロア
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： すべてのフロア')
            print(f'推定回遊人数：すべてのフロア')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            nwm_hourly_pALL_cALL(spec_comb.model, spec_comb.download, spec_comb.upload, param_dict)
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"hourly 総実行時間: {time_diff}秒")
            
        elif param_dict['hourly_estimate_migration_number'][0] == 'SEPARATE_FLOOR':
            # 人気エリア：すべてのフロア
            # 推定回遊人数：フロアごとに分ける場合
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： すべてのフロア')
            print(f'推定回遊人数：フロア別')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            nwm_hourly_pALL_cSEPARATE(spec_comb.model, spec_comb.download, spec_comb.upload, param_dict)
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"hourly 総実行時間: {time_diff}秒")
        
        else:
            # 人気エリア：すべてのフロア
            # 推定回遊人数：処理なし
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： すべてのフロア')
            print(f'推定回遊人数：処理なし')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"hourly 総実行時間: {time_diff}秒")
    
    elif param_dict['hourly_population_area'] == 'SEPARATE_FLOOR':
        if   param_dict['hourly_estimate_migration_number'][0] == 'ALL_FLOOR':
            # 人気エリア：フロアごとに分ける場合
            # 推定回遊人数：すべてのフロア
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： フロア別')
            print(f'推定回遊人数：すべてのフロア')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"hourly 総実行時間: {time_diff}秒")
            
        elif param_dict['hourly_estimate_migration_number'][0] == 'SEPARATE_FLOOR':
            # 人気エリア：フロアごとに分ける場合
            # 推定回遊人数：フロアごとに分ける場合
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： フロア別')
            print(f'推定回遊人数：フロア別')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            nwm_hourly_pSEPARATE_cSEPARATE(spec_comb.model, spec_comb.download, spec_comb.upload, param_dict)
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"hourly 総実行時間: {time_diff}秒")
        
        else:
            # 人気エリア：フロアごとに分ける場合
            # 推定回遊人数：処理なし
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： フロア別')
            print(f'推定回遊人数：処理なし')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"hourly 総実行時間: {time_diff}秒")
    
    else:
        if  param_dict['hourly_estimate_migration_number'][0] == 'ALL_FLOOR':
            # 人気エリア：処理なし
            # 推定回遊人数：すべてのフロア
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： 処理なし')
            print(f'推定回遊人数：すべてのフロア')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"hourly 総実行時間: {time_diff}秒")
        elif param_dict['hourly_estimate_migration_number'][0] == 'SEPARATE_FLOOR':
            # 人気エリア：処理なし
            # 推定回遊人数：フロアごとに分ける場合
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： 処理なし')
            print(f'推定回遊人数：フロア別')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"hourly 総実行時間: {time_diff}秒")
        
        else:
            # 人気エリア：処理なし
            # 推定回遊人数：処理なし
            print(f'NETWORKMODEL 動作区分')
            print(f'人気エリア： 処理なし')
            print(f'推定回遊人数：処理なし')
        
            # 時間計測開始
            time_start = time.perf_counter()
            
            # 時間計測完了
            time_end   = time.perf_counter()
            time_diff  = time_end - time_start
            print(f"hourly 総実行時間: {time_diff}秒")
    
    print(f'1時間単位nwm処理 終了')
    
    
    # 時間計測完了
    time_end = time.perf_counter()
    time_diff  = time_end - time_all_start
    print(f"総実行時間: {time_diff}秒")
    
    return




