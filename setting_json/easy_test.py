import re
from datetime import datetime
from typing import Literal
from dataclasses import dataclass

@dataclass
class easy_test:
    ANALYSIS_OBJECT:Literal['AI_BEACON', 'GPS_DATA']
    NETWORK_LIST_FILE:str
    USE_MODEL_TYPE:str
    ENABLE_GROUP_MODE:Literal[True, False]
    ENABLE_AIB_ROW_DATA:Literal[True, False]
    ENABLE_SPECIFY_TIME:Literal[True, False]
    ENABLE_OLD_PATH_POLICY:Literal[True, False]
    DAILY_START_TIME:str
    DAILY_END_TIME:str
    DAILY_TIME_INTERVAL:str
    DAILY_POPULATION_AREA:str
    DAILY_ESTIMATE_MIGRATION_NUMBER:list[str|int]
    DAILY_VISITOR_NUMBER:str
    HOURLY_START_TIME:str
    HOURLY_END_TIME:str
    HOURLY_TIME_INTERVAL:str
    HOURLY_POPULATION_AREA:str
    HOURLY_ESTIMATE_MIGRATION_NUMBER:list[str|int]
    HOURLY_VISITOR_NUMBER:str


# テストの最中にエラーが発生した場合には、積極的にraise文を使用する事とする
def test_project_config(ea:easy_test) -> bool:
    # project config レベルの確認
    if ea.ANALYSIS_OBJECT not in {'AI_BEACON', 'GPS_DATA'}:
        raise ValueError('There is a mistake in ANALYSIS_OBJECT.')
    
    if ea.NETWORK_LIST_FILE.endswith('.csv') is False:
        raise ValueError('The file extension of NETWORK_LIST_FILE must be .csv.')
    
    if type(ea.ENABLE_GROUP_MODE) is not bool:
        raise ValueError('ENABLE_GROUP_MODE must be a boolean value.')
    
    if type(ea.ENABLE_AIB_ROW_DATA) is not bool:
        raise ValueError('ENABLE_AIB_ROW_DATA must be a boolean value.')
    
    if type(ea.ENABLE_SPECIFY_TIME) is not bool:
        raise ValueError('ENABLE_SPECIFY_TIME must be a boolean value.')
    
    if type(ea.ENABLE_OLD_PATH_POLICY) is not bool:
        raise ValueError('ENABLE_OLD_PATH_POLICY must be a boolean value.')
    
    
    # daily config レベルの確認
    if re.fullmatch(r'^(\d{1}|\d{2}):(\d{1}|\d{2}):(\d{1}|\d{2})$', ea.DAILY_START_TIME) is None:
        raise ValueError('The format of DAILY_START_TIME must be HH:MM:SS.')
    
    if re.fullmatch(r'^(\d{1}|\d{2}):(\d{1}|\d{2}):(\d{1}|\d{2})$', ea.DAILY_END_TIME) is None:
        raise ValueError('The format of DAILY_END_TIME must be HH:MM:SS.')
    
    if datetime.strptime(ea.DAILY_START_TIME, '%H:%M:%S') >= datetime.strptime(ea.DAILY_END_TIME, '%H:%M:%S'):
        raise ValueError('DAILY_START_TIME must be earlier than DAILY_END_TIME.')
    
    if type(ea.DAILY_TIME_INTERVAL) is not str:
        raise ValueError('DAILY_TIME_INTERVAL must be a string.')
    
    if type(ea.DAILY_POPULATION_AREA) is not str:
        raise ValueError('DAILY_POPULATION_AREA must be a string.')
    
    if type(ea.DAILY_ESTIMATE_MIGRATION_NUMBER) is not list:
        raise ValueError('DAILY_ESTIMATE_MIGRATION_NUMBER must be a list.')
    
    if len(ea.DAILY_ESTIMATE_MIGRATION_NUMBER) != 2:
        raise ValueError('DAILY_ESTIMATE_MIGRATION_NUMBER must have 2 elements.')

    if type(ea.DAILY_ESTIMATE_MIGRATION_NUMBER[0]) is not str:
        raise ValueError('The first element of DAILY_ESTIMATE_MIGRATION_NUMBER must be a string.')
    
    if type(ea.DAILY_ESTIMATE_MIGRATION_NUMBER[1]) is not int:
        raise ValueError('The second element of DAILY_ESTIMATE_MIGRATION_NUMBER must be an integer.')
    
    if type(ea.DAILY_VISITOR_NUMBER) is not str:
        raise ValueError('DAILY_VISITOR_NUMBER must be a string.')
    
    
    # hourly config レベルの確認
    if re.fullmatch(r'^(\d{1}|\d{2}):(\d{1}|\d{2}):(\d{1}|\d{2})$', ea.HOURLY_START_TIME) is None:
        raise ValueError('The format of HOURLY_START_TIME must be HH:MM:SS.')
    
    if re.fullmatch(r'^(\d{1}|\d{2}):(\d{1}|\d{2}):(\d{1}|\d{2})$', ea.HOURLY_END_TIME) is None:
        raise ValueError('The format of HOURLY_END_TIME must be HH:MM:SS.')
    
    if datetime.strptime(ea.HOURLY_START_TIME, '%H:%M:%S') >= datetime.strptime(ea.HOURLY_END_TIME, '%H:%M:%S'):
        raise ValueError('HOURLY_START_TIME must be earlier than HOURLY_END_TIME.')
    
    if type(ea.HOURLY_TIME_INTERVAL) is not str:
        raise ValueError('HOURLY_TIME_INTERVAL must be a string.')
    
    if type(ea.HOURLY_POPULATION_AREA) is not str:
        raise ValueError('HOURLY_POPULATION_AREA must be a string.')
    
    if type(ea.HOURLY_ESTIMATE_MIGRATION_NUMBER) is not list:
        raise ValueError('HOURLY_ESTIMATE_MIGRATION_NUMBER must be a list.')
    
    if len(ea.HOURLY_ESTIMATE_MIGRATION_NUMBER) != 2:
        raise ValueError('HOURLY_ESTIMATE_MIGRATION_NUMBER must have 2 elements.')

    if type(ea.HOURLY_ESTIMATE_MIGRATION_NUMBER[0]) is not str:
        raise ValueError('The first element of HOURLY_ESTIMATE_MIGRATION_NUMBER must be a string.')
    
    if type(ea.HOURLY_ESTIMATE_MIGRATION_NUMBER[1]) is not int:
        raise ValueError('The second element of HOURLY_ESTIMATE_MIGRATION_NUMBER must be an integer.')
    
    if type(ea.HOURLY_VISITOR_NUMBER) is not str:
        raise ValueError('HOURLY_VISITOR_NUMBER must be a string.')
    
    return True