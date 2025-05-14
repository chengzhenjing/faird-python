import requests
import json
from typing import Optional, Dict, List
from datetime import datetime
from pydantic import ValidationError

from core.config import FairdConfig, FairdConfigManager
from core.models.dataset import DataSet
from core.models.dataset_meta import DatasetMetadata
from services.datasource.interfaces.datasource_interface import FairdDatasourceInterface

class MetaCatService(FairdDatasourceInterface):
    """
    读取MetaCat数据目录服务
    """
    metacat_url = "http://10.0.82.71:8080"  # MetaCat服务的URL
    metacat_token = "your_metacat_token"  # MetaCat服务的访问令牌
    datasets = {}  # 数据集ID和名称的映射
    dataset_count = 0  # 数据集总数量

    def __init__(self):
        super().__init__()
        self.config = FairdConfigManager.get_config()
        self.metacat_url = self.config.metacat_url
        self.metacat_token = self.config.metacat_token

    def list_dataset(self, token: str, page: int = 1, limit: int = 10) -> List[str]:
        url = f"{self.metacat_url}/api/fair/listDatasets"

        headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }

        params = {
            "page": page,
            "limit": limit
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # 检查请求是否成功
            if response.status_code != 200:
                print(f"Error fetching dataset list: {response.status_code}")
                return None
            data = response.json().get("data")

            # 解析JSON字符串
            dataset_list = data.get("datasetIds", [])
            self.dataset_count = data.get("count", 0)

            # 更新全局的datasets字典
            for dataset in dataset_list:
                self.datasets[dataset['id']] = dataset['name']

            return dataset_list
        except requests.RequestException as e:
            print(f"Error fetching dataset list: {e}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}")
        except KeyError as e:
            print(f"Error parsing response: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
        
        return None

    def fetch_dataset_details(self, token: str, username: str, dataset_name: str) -> Optional[DataSet]:

        url = f"{self.metacat_url}/metacat/getDatasetById"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # 获取数据集ID
        dataset_id = get_key(self.datasets, dataset_name)
        if dataset_id is None:
            print(f"dataset {dataset_name} not found in the dataset list.")
            return None

        params = {
            "datasetId": dataset_id
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status() # 检查请求是否成功
            if response.status_code != 200:
                print(f"Error fetching dataset details: {response.status_code}")
                return None

            data = response.json()
            metadata_obj = data.get("data", "{}").get("metadata", {})
            dataframeIds_obj = data.get("data", "{}").get("dataframeIds", [])
            accessInfo_obj = data.get("data", "{}").get("access_info", {})

            has_permission = self._check_permission(token, dataset_id, username)  # 检查用户是否有数据集权限
            
            # 创建并返回DataSet对象
            dataset = DataSet(
                meta=parse_metadata(metadata_obj),
                dataframeIds=dataframeIds_obj,
                accessible=has_permission,
                accessInfo=accessInfo_obj if has_permission else None
            )
            return dataset
        except Exception as e:
            print(f"Error fetching dataset info from metacat: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON resonse: {e}")
            return None
        except KeyError as e:
            print(f"Error parsing response: {e}")
            return None
    
    def _check_permission(self, token: str, dataset_id: str, username: str) -> bool:
        """
        检查用户对数据集的访问权限(默认为无)
        @param dataset_id: 数据集ID
        @param username: 用户名
        @return: 是否有权限访问
        """
        url = f"{self.metacat_url}/metacat/checkPermission"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        params = {
            "username": username,
            "datasetId": dataset_id
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json().get("data", {}).get("result", False)
        except Exception as e:
            print(f"Error checking permission: {e}")
            return False

def parse_metadata(raw_data: dict) -> Optional[DatasetMetadata]:
    """解析元数据字段"""
    processed_data = raw_data.copy()
    
    # 转换分号分隔的字符串为列表
    if "basic" in processed_data:
        basic = processed_data["basic"]
        if "keywords" in basic and isinstance(basic["keywords"], str):
            basic["keywords"] = basic["keywords"].split(";")
        
        # 转换日期字符串
        if "datePublished" in basic:
            date_str = basic["datePublished"]
            basic["datePublished"] = datetime.strptime(date_str, "%Y-%m-%d").date()
    
    # 解析为DatasetMetadata对象
    try:
        dataset_metadata = DatasetMetadata.model_validate(processed_data)
        print("元数据解析成功:", dataset_metadata)
        return dataset_metadata
    except ValidationError as e:
        print(f"元数据解析失败:\n{e.json()}")
        return None

def get_key(dictionary, value):
    """根据值获取字典的键"""
    try:
        return {val: key for key, val in dictionary.items()}[value]
    except KeyError:
        print(f"Value {value} not found in dictionary.")
        return None
