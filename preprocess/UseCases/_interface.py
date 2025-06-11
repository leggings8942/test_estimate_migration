from abc import ABCMeta, abstractmethod
from pyspark.sql import DataFrame

#csvファイル保存用インターフェース
class upload_to_file(metaclass=ABCMeta):
    @abstractmethod
    def write_parquet_date_file(self, path:str, ps_df:DataFrame) -> None:
        raise NotImplementedError()