from pyspark.sql import DataFrame
import IO_control.export_file as ex
from UseCases._interface import upload_to_file

    

class original_UL(upload_to_file):
    def __init__(self, DATABRICKS_PREFIX:str, PROJECT_PATH:str, PREPROCESS_PATH:str, date:str) -> None:
        super().__init__()
        self.PREFIX      = DATABRICKS_PREFIX
        self.OUTPUT_PATH = PROJECT_PATH + '/' + PREPROCESS_PATH
        self.DATE        = date
    
    def write_parquet_date_file(self, path:str, ps_df:DataFrame) -> None:
        output_path = self.OUTPUT_PATH + path
        ex.write_parquet_date_file(self.PREFIX, output_path, self.DATE, ps_df)