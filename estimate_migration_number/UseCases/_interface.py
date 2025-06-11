from abc import ABCMeta, abstractmethod
from typing import Literal
from pyspark.sql import DataFrame

# 推定回遊人数計算用インターフェース
class calc_estimate_migration(metaclass=ABCMeta):
    @abstractmethod
    def read_visitor_number(self, migration_type:Literal['ALL_FLOOR', 'SEPARATE_FLOOR'], section_id:str, GRANULARITY:Literal['daily', 'hourly']) -> DataFrame:
        raise NotImplementedError()
    
    @abstractmethod
    def read_causality(self, migration_type:Literal['ALL_FLOOR', 'SEPARATE_FLOOR'], section_id:str, GRANULARITY:Literal['daily', 'hourly'], level:Literal[2, 3, 4]) -> DataFrame:
        raise NotImplementedError()
    
    @abstractmethod
    def write_estimate_migration_number(self, migration_type:Literal['ALL_FLOOR', 'SEPARATE_FLOOR'], section_id:str, GRANULARITY:Literal['daily', 'hourly'], level:Literal[2, 3, 4], df_move_pop:DataFrame) -> None:
        raise NotImplementedError()