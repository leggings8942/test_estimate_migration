import time

from .Entities.estimate_migration_number import get_estimate_migration_number_daily
from .Entities.estimate_migration_number import get_estimate_migration_number_hourly
from ._interface import calc_estimate_migration



'''
1日単位回遊人数の解析
影響量(Causal quantity)： すべてのフロア
'''
def calc_estimate_migration_number_daily_ALL(model:calc_estimate_migration, param_dict:dict) -> None:
    # param_dictに期待する構成
    # dict
    #  |-- date                             : date
    #  |-- part_id                          : str
    #  |-- daily_estimate_migration_number  : list[str | int],
    date          = param_dict['date']                               # 解析対象日
    part_id       = param_dict['part_id']                            # 解析対象のパートID
    migration_num = param_dict['daily_estimate_migration_number'][1] # 必要回遊階層数
    


    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}:: Part_ID:{part_id}  日別推定来訪者数データ準備時間 計測開始")
    
    # 日別推定来訪者数を取得
    df_visit_cnt = model.read_visitor_number('ALL_FLOOR', '', 'daily')
    
    # 時間計測完了
    time_end  = time.perf_counter()
    time_diff = time_end - time_start
    print(f"{date}:: Part_ID:{part_id}  日別推定来訪者数データ準備時間 {time_diff}秒")
    
    
    # 指定の階層数分ループ
    for level in range(2, migration_num + 1):
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id} 対象回遊数:{level}  日別指定階層のIRF(因果量)データ準備時間 計測開始")
        
        # =======================================================================
        # 推定回遊人数計算のための必要ファイルの読み込み
        # =======================================================================
        df_causality = model.read_causality('ALL_FLOOR', '', 'daily', level)
        
        # 時間計測完了
        time_end  = time.perf_counter()
        time_diff = time_end - time_start
        print(f"{date}:: Part_ID:{part_id} 対象回遊数:{level}  日別指定階層のIRF(因果量)データ準備時間 {time_diff}秒")
        
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id} 対象回遊数:{level}  日別指定階層のIRF(因果量)データ解析時間 計測開始")

        # =======================================================================
        # 推定回遊人数の計算
        # =======================================================================
        influence = 'MOVEMENT_INFLUENCE_AMOUNT_VIA_' + str(level - 2)
        if level == 2:
            influence = 'MOVEMENT_INFLUENCE_AMOUNT'
        
        df_migrate_num = get_estimate_migration_number_daily(influence, df_visit_cnt, df_causality)
        
        # 時間計測完了
        time_end  = time.perf_counter()
        time_diff = time_end - time_start
        print(f"{date}:: Part_ID:{part_id} 対象回遊数:{level}  日別指定階層のIRF(因果量)データ解析時間 {time_diff}秒")


        # =======================================================================
        # 推定回遊人数の出力
        # =======================================================================
        model.write_estimate_migration_number('ALL_FLOOR', '', 'daily', level, df_migrate_num)
    
    return None

'''
1日単位回遊人数の解析
影響量(Causal quantity)： フロア別
'''
def calc_estimate_migration_number_daily_SEPARATE(model:calc_estimate_migration, param_dict:dict) -> None:
    # param_dictに期待する構成
    # dict
    #  |-- date                             : date
    #  |-- part_id                          : str
    #  |-- section_id_list                  : list[str],
    #  |-- daily_estimate_migration_number  : list[str | int],
    date          = param_dict['date']                               # 解析対象日
    part_id       = param_dict['part_id']                            # 解析対象のパートID
    sn_id_list    = param_dict['section_id_list']                    # セクションIDリスト
    migration_num = param_dict['daily_estimate_migration_number'][1] # 必要回遊階層数
    

        
    # セクションIDごとにループ
    for idx, section_id in enumerate(sn_id_list):
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  日別推定来訪者数データ準備時間 計測開始")
        
        # 日別推定来訪者数を取得
        df_visit_cnt = model.read_visitor_number('SEPARATE_FLOOR', section_id, 'daily')
        
        # 時間計測完了
        time_end  = time.perf_counter()
        time_diff = time_end - time_start
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  日別推定来訪者数データ準備時間 {time_diff}秒")
        

        # 指定の階層数分ループ
        for level in range(2, migration_num + 1):
            # 時間計測開始
            time_start = time.perf_counter()
            print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id} 対象回遊数:{level}  日別指定階層のIRF(因果量)データ準備時間 計測開始")

            # =======================================================================
            # 推定回遊人数計算のための必要ファイルの読み込み
            # =======================================================================
            df_causality = model.read_causality('SEPARATE_FLOOR', section_id, 'daily', level)
            
            # 時間計測完了
            time_end  = time.perf_counter()
            time_diff = time_end - time_start
            print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id} 対象回遊数:{level}  日別指定階層のIRF(因果量)データ準備時間 {time_diff}秒")

            
            # 時間計測開始
            time_start = time.perf_counter()
            print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id} 対象回遊数:{level}  日別指定階層のIRF(因果量)データ解析時間 計測開始")

            # =======================================================================
            # 推定回遊人数の計算
            # =======================================================================
            influence = 'MOVEMENT_INFLUENCE_AMOUNT_VIA_' + str(level - 2)
            if level == 2:
                influence = 'MOVEMENT_INFLUENCE_AMOUNT'

            df_migrate_num = get_estimate_migration_number_daily(influence, df_visit_cnt, df_causality)
            
            # 時間計測完了
            time_end  = time.perf_counter()
            time_diff = time_end - time_start
            print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id} 対象回遊数:{level}  日別指定階層のIRF(因果量)データ解析時間 {time_diff}秒")

            
            # =======================================================================
            # 推定回遊人数の出力
            # =======================================================================
            model.write_estimate_migration_number('SEPARATE_FLOOR', section_id, 'daily', level, df_migrate_num)
    
    return None


'''
1時間単位回遊人数の解析
影響量(Causal quantity)： すべてのフロア
'''
def calc_estimate_migration_number_hourly_ALL(model:calc_estimate_migration, param_dict:dict) -> None:
    # param_dictに期待する構成
    # dict
    #  |-- date                             : date
    #  |-- part_id                          : str
    #  |-- hourly_estimate_migration_number : list[str, int],
    date          = param_dict['date']                                # 解析対象日
    part_id       = param_dict['part_id']                             # 解析対象のパートID
    migration_num = param_dict['hourly_estimate_migration_number'][1] # 必要回遊階層数
    
    
    
    # 時間計測開始
    time_start = time.perf_counter()
    print(f"{date}:: Part_ID:{part_id}  時間別推定来訪者数データ準備時間 計測開始")
    
    # 時間別推定来訪者数を取得
    df_visit_cnt = model.read_visitor_number('ALL_FLOOR', '', 'hourly')
    
    # 時間計測完了
    time_end  = time.perf_counter()
    time_diff = time_end - time_start
    print(f"{date}:: Part_ID:{part_id}  時間別推定来訪者数データ準備時間 {time_diff}秒")
    
    
    # 指定の階層数分ループ
    for level in range(2, migration_num + 1):
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id} 対象回遊数:{level}  時間別指定階層のIRF(因果量)データ準備時間 計測開始")
    
        # =======================================================================
        # 推定回遊人数計算のための必要ファイルの読み込み
        # =======================================================================
        df_causality = model.read_causality('ALL_FLOOR', '', 'hourly', level)
        
        # 時間計測完了
        time_end  = time.perf_counter()
        time_diff = time_end - time_start
        print(f"{date}:: Part_ID:{part_id} 対象回遊数:{level}  時間別指定階層のIRF(因果量)データ準備時間 {time_diff}秒")
        
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id} 対象回遊数:{level}  時間別指定階層のIRF(因果量)データ解析時間 計測開始")

        # =======================================================================
        # 推定回遊人数の計算
        # =======================================================================
        influence = 'MOVEMENT_INFLUENCE_AMOUNT_VIA_' + str(level - 2)
        if level == 2:
            influence = 'MOVEMENT_INFLUENCE_AMOUNT'
        
        df_migrate_num = get_estimate_migration_number_hourly(influence, df_visit_cnt, df_causality)
        
        # 時間計測完了
        time_end  = time.perf_counter()
        time_diff = time_end - time_start
        print(f"{date}:: Part_ID:{part_id} 対象回遊数:{level}  時間別指定階層のIRF(因果量)データ解析時間 {time_diff}秒")
        

        # =======================================================================
        # 推定回遊人数の出力
        # =======================================================================
        model.write_estimate_migration_number('ALL_FLOOR', '', 'hourly', level, df_migrate_num)
    
    return None

'''
1時間単位回遊人数の解析
影響量(Causal quantity)： フロア別
'''
def calc_estimate_migration_number_hourly_SEPARATE(model:calc_estimate_migration, param_dict:dict) -> None:
    # param_dictに期待する構成
    # dict
    #  |-- date                             : date
    #  |-- part_id                          : str
    #  |-- section_id_list                  : list[str],
    #  |-- hourly_estimate_migration_number : list[str, int],
    date          = param_dict['date']                               # 解析対象日
    part_id       = param_dict['part_id']                            # 解析対象のパートID
    sn_id_list    = param_dict['section_id_list']                     # セクションIDリスト
    migration_num = param_dict['hourly_estimate_migration_number'][1] # 必要回遊階層数
    
    
        
    # セクションIDごとにループ
    for idx, section_id in enumerate(sn_id_list):
        
        # 時間計測開始
        time_start = time.perf_counter()
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  時間別推定来訪者数データ準備時間 計測開始")
        
        # 日別推定来訪者数を取得
        df_visit_cnt = model.read_visitor_number('SEPARATE_FLOOR', section_id, 'hourly')
        
        # 時間計測完了
        time_end  = time.perf_counter()
        time_diff = time_end - time_start
        print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id}  時間別推定来訪者数データ準備時間 {time_diff}秒")
        

        # 指定の階層数分ループ
        for level in range(2, migration_num + 1):
            # 時間計測開始
            time_start = time.perf_counter()
            print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id} 対象回遊数:{level}  時間別指定階層のIRF(因果量)データ準備時間 計測開始")
            
            # =======================================================================
            # 推定回遊人数計算のための必要ファイルの読み込み
            # =======================================================================
            df_causality = model.read_causality('SEPARATE_FLOOR', section_id, 'hourly', level)
            
            # 時間計測完了
            time_end  = time.perf_counter()
            time_diff = time_end - time_start
            print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id} 対象回遊数:{level}  時間別指定階層のIRF(因果量)データ準備時間 {time_diff}秒")
            
            
            # 時間計測開始
            time_start = time.perf_counter()
            print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id} 対象回遊数:{level}  時間別指定階層のIRF(因果量)データ解析時間 計測開始")

            # =======================================================================
            # 推定回遊人数の計算
            # =======================================================================
            influence = 'MOVEMENT_INFLUENCE_AMOUNT_VIA_' + str(level - 2)
            if level == 2:
                influence = 'MOVEMENT_INFLUENCE_AMOUNT'

            df_migrate_num = get_estimate_migration_number_hourly(influence, df_visit_cnt, df_causality)
            
            # 時間計測完了
            time_end  = time.perf_counter()
            time_diff = time_end - time_start
            print(f"{date}:: Part_ID:{part_id} Section-{idx+1}:{section_id} 対象回遊数:{level}  時間別指定階層のIRF(因果量)データ解析時間 {time_diff}秒")


            # =======================================================================
            # 推定回遊人数の出力
            # =======================================================================
            model.write_estimate_migration_number('SEPARATE_FLOOR', section_id, 'hourly', level, df_migrate_num)
    
    return None


def calc_estimate_migration_number(model:calc_estimate_migration, param_dict:dict) -> None:
    # param_dictに期待する構成
    # dict
    #  |-- date                             : date
    #  |-- part_id                          : str
    #  |-- section_id_list                  : list[str]
    #  |-- daily_estimate_migration_number  : list[str | int]
    #  |-- hourly_estimate_migration_number : list[str | int]
    
    
    
    # 時間計測開始
    time_all_start = time.perf_counter()
    
    
    #====================================================================
    # 1日単位回遊人数推定処理
    #====================================================================
    print(f'1日単位回遊人数推定処理 開始')
    
    if   param_dict['daily_estimate_migration_number'][0] == 'ALL_FLOOR':
        # 推定回遊人数：すべてのフロア
        print(f'動作区分')
        print(f'推定回遊人数：すべてのフロア')
        
        # 時間計測開始
        time_start = time.perf_counter()
        
        calc_estimate_migration_number_daily_ALL(model, param_dict)
        
        # 時間計測完了
        time_end   = time.perf_counter()
        time_diff  = time_end - time_start
        print(f"daily 総実行時間: {time_diff}秒")
            
    elif param_dict['daily_estimate_migration_number'][0] == 'SEPARATE_FLOOR':
        # 推定回遊人数：フロアごとに分ける場合
        print(f'動作区分')
        print(f'推定回遊人数：フロア別')
        
        # 時間計測開始
        time_start = time.perf_counter()
        
        calc_estimate_migration_number_daily_SEPARATE(model, param_dict)
        
        # 時間計測完了
        time_end   = time.perf_counter()
        time_diff  = time_end - time_start
        print(f"daily 総実行時間: {time_diff}秒")
        
    else:
        # 推定回遊人数：処理なし
        print(f'動作区分')
        print(f'推定回遊人数：処理なし')
        
        # 時間計測開始
        time_start = time.perf_counter()
        
        # 時間計測完了
        time_end   = time.perf_counter()
        time_diff  = time_end - time_start
        print(f"daily 総実行時間: {time_diff}秒")
    
    print(f'1日単位回遊人数推定処理 終了')
    
    
    
    #====================================================================
    # 1時間単位回遊人数推定処理
    #====================================================================
    print(f'1時間単位回遊人数推定処理 開始')
    
    if   param_dict['hourly_estimate_migration_number'][0] == 'ALL_FLOOR':
        # 推定回遊人数：すべてのフロア
        print(f'動作区分')
        print(f'推定回遊人数：すべてのフロア')
        
        # 時間計測開始
        time_start = time.perf_counter()
        
        calc_estimate_migration_number_hourly_ALL(model, param_dict)
        
        # 時間計測完了
        time_end   = time.perf_counter()
        time_diff  = time_end - time_start
        print(f"hourly 総実行時間: {time_diff}秒")
        
    elif param_dict['hourly_estimate_migration_number'][0] == 'SEPARATE_FLOOR':
        # 推定回遊人数：フロアごとに分ける場合
        print(f'動作区分')
        print(f'推定回遊人数：フロア別')
        
        # 時間計測開始
        time_start = time.perf_counter()
        
        calc_estimate_migration_number_hourly_SEPARATE(model, param_dict)
        
        # 時間計測完了
        time_end   = time.perf_counter()
        time_diff  = time_end - time_start
        print(f"hourly 総実行時間: {time_diff}秒")
    
    else:
        # 推定回遊人数：処理なし
        print(f'動作区分')
        print(f'推定回遊人数：処理なし')
        
        # 時間計測開始
        time_start = time.perf_counter()
        
        # 時間計測完了
        time_end   = time.perf_counter()
        time_diff  = time_end - time_start
        print(f"hourly 総実行時間: {time_diff}秒")
            
    
    print(f'1時間単位回遊人数推定処理 終了')
    
    
    # 時間計測完了
    time_end = time.perf_counter()
    time_diff  = time_end - time_all_start
    print(f"総実行時間: {time_diff}秒")
    
    
    return None
    


