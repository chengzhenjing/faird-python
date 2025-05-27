from abc import ABC, abstractmethod
from typing import Optional, Dict, List
# from services.datasource.types import UrlElement
from core.models.dataset import DataSet
from core.models.dataset_meta import DatasetMetadata


class FairdDatasourceInterface(ABC):
    
    """
    加载数据集列表
    @return: 数据集名称列表
    """
    @abstractmethod
    def list_dataset(self, token: str, page: int = 1, limit: int = 10) -> List[str]:
        pass

    @abstractmethod
    def get_dataset_meta(self, token: str, dataset_name: str) -> Optional[DatasetMetadata]:
        pass

    """
    加载给定的数据集Details(包括metadata、dataframeIds、访问权限accessible、访问信息accessInfo)
    @param dataset_name: 数据中心内部数据集名称
    """
    @abstractmethod
    def list_dataframes(self, token: str, dataset_name: str):
        pass


    # """
    # 加载给定的dataframe实体数据
    # @param dataframe_id: 实体文件名称
    # @return: 实体数据表(DataFrame)的定位符
    # """
    # @abstractmethod
    # def get_data(self, dataframe_id: str) -> Optional['str']:
    #     pass


    # """
    # 用户认证
    # @param username: 用户名
    # @param password: 密码
    # @return: 认证结果
    # """
    # @abstractmethod
    # def authenticate(self, username: str, password: str) -> Optional['bool']:
    #     pass

