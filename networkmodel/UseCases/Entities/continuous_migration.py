import pandas as pd

'''
3回遊の計算処理
'''
def calc_migration(df_devided:pd.DataFrame) -> pd.DataFrame:
    # df_deviedに期待する構成
    #  | 'date'    | 'ORIGIN' | 'DESTINATION' | 'MOVEMENT_INFLUENCE_AMOUNT'
    # 0, 2022-10-01, 30943,     30943,          0.0
    # 1, 2022-10-01, 30943,     30984,          0.1013
    # 2, 2022-10-01, 30943,     30985,          0.1043
    # 3, 2022-10-01, 30943,     31177,          0.0
    # ・
    # ・
    # ・
    
    # 移動影響量が0のデータを省く
    df_devided   = df_devided[df_devided['MOVEMENT_INFLUENCE_AMOUNT'] != 0]
    
    # 経由地1のunitリストと時刻リスト
    v_unit_list  = df_devided['DESTINATION'].drop_duplicates().to_list()
    v_time_list  = df_devided['date'].drop_duplicates().to_list()

    # 回遊計算済みのDataFrameリスト
    migrate_list = [pd.DataFrame(columns=['date', 'ORIGIN', 'VIA_1', 'DESTINATION', 'MOVEMENT_INFLUENCE_AMOUNT_VIA_1'])]
    for v_time in v_time_list:
        tmp_devided = df_devided[  df_devided['date']   == v_time]
        
        for v_unit in v_unit_list:
            # 1) 2階層回遊におけるDESTINATIONのunitを抽出
            df_v1_to_d = tmp_devided[tmp_devided['DESTINATION'] == v_unit].rename(columns={'MOVEMENT_INFLUENCE_AMOUNT':'MOVEMENT_INFLUENCE_AMOUNT_VIA_0'})
            df_v1_to_d = df_v1_to_d[['date',  'ORIGIN',      'MOVEMENT_INFLUENCE_AMOUNT_VIA_0']]
        
            # 2) 1)のunitを出発地とするirfを抽出
            df_v1_to_o = tmp_devided[tmp_devided['ORIGIN']      == v_unit].rename(columns={'ORIGIN':'VIA_1', 'MOVEMENT_INFLUENCE_AMOUNT':'MOVEMENT_INFLUENCE_AMOUNT_VIA_1'})
            df_v1_to_o = df_v1_to_o[['VIA_1', 'DESTINATION', 'MOVEMENT_INFLUENCE_AMOUNT_VIA_1']]
        
            # 3) 交差結合する
            df_v1 = pd.merge(df_v1_to_d, df_v1_to_o, how='cross')
        
            # 4) 出発地・経由地1のirf と 経由地1・目的地のirf を掛ける
            df_v1['MOVEMENT_INFLUENCE_AMOUNT_VIA_1'] = df_v1['MOVEMENT_INFLUENCE_AMOUNT_VIA_0'] * df_v1['MOVEMENT_INFLUENCE_AMOUNT_VIA_1']
        
            # 5) 列を整える
            df_v1 = df_v1.reindex(columns=['date', 'ORIGIN', 'VIA_1', 'DESTINATION', 'MOVEMENT_INFLUENCE_AMOUNT_VIA_1'])
        
            # 6) リストに登録する
            migrate_list.append(df_v1)

    # 縦結合する
    df_migrate = pd.concat(migrate_list, axis=0, ignore_index=True)
    df_migrate = df_migrate.sort_values(['date', 'ORIGIN', 'VIA_1', 'DESTINATION'], ignore_index=True)
    # df_migrateに期待する構成
    #  | 'date'    | 'ORIGIN' | 'VIA_1' | 'DESTINATION' | 'MOVEMENT_INFLUENCE_AMOUNT_VIA_1'
    # 0, 2022-10-01, 30943,     30984,    30943,          0.0
    # 1, 2022-10-01, 30943,     30984,    30984,          0.01087849
    # 2, 2022-10-01, 30943,     30984,    30985,          0.01087849
    # 3, 2022-10-01, 30943,     30984,    31177,          0.0
    # ・
    # ・
    # ・
    
    return df_migrate


'''
4回遊の計算処理
'''
def calc_migration_fourth(df_devided:pd.DataFrame, df_migration:pd.DataFrame) -> pd.DataFrame:
    # df_deviedに期待する構成
    #  | 'date'    | 'ORIGIN' | 'DESTINATION' | 'MOVEMENT_INFLUENCE_AMOUNT'
    # 0, 2022-10-01, 30943,     30943,          0.0
    # 1, 2022-10-01, 30943,     30984,          0.1013
    # 2, 2022-10-01, 30943,     30985,          0.1043
    # 3, 2022-10-01, 30943,     31177,          0.0
    # ・
    # ・
    # ・
    
    # df_migrationに期待する構成
    #  | 'date'    | 'ORIGIN' | 'VIA_1' | 'DESTINATION' | 'MOVEMENT_INFLUENCE_AMOUNT_VIA_1'
    # 0, 2022-10-01, 30943,     30984,    30943,          0.0
    # 1, 2022-10-01, 30943,     30984,    30984,          0.01087849
    # 2, 2022-10-01, 30943,     30984,    30985,          0.01087849
    # 3, 2022-10-01, 30943,     30984,    31177,          0.0
    # ・
    # ・
    # ・
    
    # 移動影響量が0のデータを省く
    df_devided   = df_devided[  df_devided[  'MOVEMENT_INFLUENCE_AMOUNT']       != 0]
    df_migration = df_migration[df_migration['MOVEMENT_INFLUENCE_AMOUNT_VIA_1'] != 0]
    
    # 経由地2のunitリストと時刻リスト
    o_unit_list  = df_migration['DESTINATION'].drop_duplicates().to_list()
    o_time_list  = df_migration['date'].drop_duplicates().to_list()
    
    # 回遊計算済みのDataFrameリスト
    migrate_list = [pd.DataFrame(columns=['date', 'ORIGIN', 'VIA_1', 'VIA_2', 'DESTINATION', 'MOVEMENT_INFLUENCE_AMOUNT_VIA_2'])]
    for o_time in o_time_list:
        tmp_migration = df_migration[df_migration['date'] == o_time]
        tmp_devided   = df_devided[  df_devided['date']   == o_time]
        
        for o_unit in o_unit_list:
            # 1) 3階層回遊におけるDESTINATIONのunitを抽出
            df_v2_to_d = tmp_migration[tmp_migration['DESTINATION'] == o_unit]
            df_v2_to_d = df_v2_to_d[['date',  'ORIGIN',      'VIA_1', 'MOVEMENT_INFLUENCE_AMOUNT_VIA_1']]
        
            # 2) 1)のunitを出発地とするirfを抽出
            df_v2_to_o = tmp_devided[  tmp_devided['ORIGIN']        == o_unit].rename(columns={'ORIGIN':'VIA_2', 'MOVEMENT_INFLUENCE_AMOUNT':'MOVEMENT_INFLUENCE_AMOUNT_VIA_2'})
            df_v2_to_o = df_v2_to_o[['VIA_2', 'DESTINATION',          'MOVEMENT_INFLUENCE_AMOUNT_VIA_2']]
        
            # 3) 交差結合する
            df_v2 = pd.merge(df_v2_to_d, df_v2_to_o, how='cross')
        
            # 4) 出発地・経由地1のirf と 経由地1・経由地2のirf と 経由地2・目的地のirf を掛ける
            df_v2['MOVEMENT_INFLUENCE_AMOUNT_VIA_2'] = df_v2['MOVEMENT_INFLUENCE_AMOUNT_VIA_1'] * df_v2['MOVEMENT_INFLUENCE_AMOUNT_VIA_2']
        
            # 5) 列を整える
            df_v2 = df_v2.reindex(columns=['date', 'ORIGIN', 'VIA_1', 'VIA_2', 'DESTINATION', 'MOVEMENT_INFLUENCE_AMOUNT_VIA_2'])
        
            # 6) リストに登録する
            migrate_list.append(df_v2)

    
    # 縦結合する
    df_migrate = pd.concat(migrate_list, axis=0, ignore_index=True)
    df_migrate = df_migrate.sort_values(['date', 'ORIGIN', 'VIA_1', 'VIA_2', 'DESTINATION'], ignore_index=True)
    # df_migrateに期待する構成
    #  | 'date'    | 'ORIGIN' | 'VIA_1' | 'VIA_2' | 'DESTINATION' | 'MOVEMENT_INFLUENCE_AMOUNT_VIA_2'
    # 0, 2022-10-01, 30943,     30984,    30985,    30943,          0.0
    # 1, 2022-10-01, 30943,     30984,    30985,    30984,          0.00103951
    # 2, 2022-10-01, 30943,     30984,    30985,    30985,          0.00103951
    # 3, 2022-10-01, 30943,     30984,    30985,    31177,          0.0
    # ・
    # ・
    # ・

    return df_migrate


'''
3回遊の計算結果を返す関数
'''
def get_migration_three(df:pd.DataFrame) -> pd.DataFrame:
    # 3回遊の計算処理
    df_migrate = calc_migration(df)
    
    return df_migrate

'''
4回遊の計算結果を返す関数
'''
def get_migration_fourth(df:pd.DataFrame) -> pd.DataFrame:
    # 3回遊の計算処理
    df_migrate = calc_migration(df)
    
    # 4回遊の計算処理
    df_migrate_fourth = calc_migration_fourth(df, df_migrate)
    
    return df_migrate_fourth

'''
3回遊・4回遊の計算結果を返す関数
'''
def get_migration(df:pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # 3回遊の計算処理
    df_migrate_three = calc_migration(df)
    
    # 4回遊の計算処理
    df_migrate_fourth = calc_migration_fourth(df, df_migrate_three)
    
    return df_migrate_three, df_migrate_fourth