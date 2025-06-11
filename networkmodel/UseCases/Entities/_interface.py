from abc import ABCMeta, abstractmethod
import numpy as np
import pandas as pd

#時系列分析用インターフェース
class time_series_model(metaclass=ABCMeta):
    @abstractmethod
    def register(self, data:pd.DataFrame) -> bool:
        raise NotImplementedError()
    
    @abstractmethod
    def fit(self, lags:int, offset:int, solver:str) -> bool:
        raise NotImplementedError()
    
    @abstractmethod
    def select_order(self, maxlag:int, ic:str, solver:str) -> int:
        raise NotImplementedError()
    
    @abstractmethod
    def irf(self, period:int, orth:bool, isStdDevShock:bool) -> np.ndarray[np.float64]:
        raise NotImplementedError()
    
    @abstractmethod
    def get_coef(self) -> tuple[np.ndarray[np.float64], np.ndarray[np.float64]]:
        raise NotImplementedError()
