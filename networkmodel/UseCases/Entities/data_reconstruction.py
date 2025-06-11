import pandas as pd



'''
セクション辞書の構成を組み直して返す関数
セクション辞書[セクションID,ユニークID] -> ユニーク辞書[ユニークID,セクションID]
'''
def get_unique_to_section_dict(sn_id_dict:dict) -> dict:
    
    # ユニークID単位のセクションID辞書を作成
    unique_id_dict = {}
    for section_id in sn_id_dict.keys():
        unique_id_list = sn_id_dict[section_id]
        
        # ユニークIDとセクションIDの対応を登録
        for unique_id in unique_id_list:
            unique_id_dict[unique_id] = section_id
    
    return unique_id_dict


'''
サブセクション辞書の構成を組み直して返す関数
サブセクション辞書[セクションID,サブセクションID,ユニークID] -> サブセクション辞書[サブセクションID,セクションID]
'''
def get_subsection_to_section_dict(sn_ssn_id_dict:dict) -> dict:
    
    # サブセクション単位のセクションID辞書を作成
    section_id_dict = {}
    for section_id in sn_ssn_id_dict.keys():
        ssn_id_dict = sn_ssn_id_dict[section_id]
        
        # サブセクションとセクションIDの対応を登録
        for subsection_id in ssn_id_dict.keys():
            if subsection_id not in section_id_dict:
                section_id_dict[subsection_id] = []
            
            section_id_dict[subsection_id].append(section_id)
    
    return section_id_dict


'''
サブセクション辞書の構成を組み直して返す関数
サブセクション辞書[セクションID,サブセクションID,ユニークID] -> サブセクション辞書[サブセクションID,ユニークID]
'''
def get_subsection_to_unique_dict(sn_ssn_id_dict:dict) -> dict:
        
    # サブセクション単位のユニークID辞書を作成
    subsection_id_dict = {}
    for section_id in sn_ssn_id_dict.keys():
        ssn_id_dict = sn_ssn_id_dict[section_id]
        
        # サブセクションとユニークIDの対応を登録
        for subsection_id in ssn_id_dict.keys():
            if subsection_id not in subsection_id_dict:
                subsection_id_dict[subsection_id] = []
            
            subsection_id_dict[subsection_id].extend(ssn_id_dict[subsection_id])
    
    return subsection_id_dict