from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
import pandas as pd
from typing import Literal

from .Entities._interface import time_series_model

#csvファイル保存用インターフェース
class upload_to_file(metaclass=ABCMeta):
    @abstractmethod
    def debug_write_csv_file(self, SPLIT_MODE:Literal['ALL', 'SEPARATE'], section_id:str, file_path:str, df:pd.DataFrame, f_name:str, need_index:bool=False) -> None:
        raise NotImplementedError()
    
    @abstractmethod
    def write_popular_area(self, SPLIT_MODE:Literal['ALL', 'SEPARATE'], section_id:str, GRANULARITY:Literal['daily', 'hourly'], file_path:str, df:pd.DataFrame, f_name:str) -> None:
        raise NotImplementedError()

#csvファイル取得用インターフェース
class download_to_file(metaclass=ABCMeta):
    @abstractmethod
    def read_csv_file(self, path:str) -> pd.DataFrame:
        raise NotImplementedError()
    
    @abstractmethod
    def read_parquet_for_1min(self, date:str, ueid_list:list[str]) -> pd.DataFrame:
        raise NotImplementedError()

# 構造体
@dataclass
class UseInterface:
    model:    time_series_model
    upload:   upload_to_file
    download: download_to_file
