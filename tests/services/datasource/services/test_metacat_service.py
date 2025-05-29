import os
import unittest
from unittest.mock import patch, MagicMock
from services.datasource.services.metacat_service import MetaCatService


class TestMetaCatService(unittest.TestCase):

    from core.config import FairdConfigManager
    FairdConfigManager.load_config(os.path.join("/Users/zjcheng/Documents/zjcheng_macbookpro/projects/faird", 'faird.conf'))

    @patch("services.datasource.services.metacat_service.requests.get")
    def test_list_dataset(self, mock_get):
        # 模拟请求返回值
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "datasetIds": '[{"name": "数据集01", "id": "ds001"}, {"name": "数据集02", "id": "ds002"}]',
            "count": 2 ## 所有数据集总数
        }

        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        service = MetaCatService()
        result = service.list_dataset(token="", page=1, limit=10)

        # 验证结果
        self.assertEqual(len(result), 2)
        self.assertEqual(service.dataset_count, 2)
        self.assertIn("ds001", service.datasets)
        self.assertEqual(service.datasets["ds001"], "数据集01")

    @patch("services.datasource.services.metacat_service.requests.get")
    def test_fetch_dataset_details(self, mock_get):
        # 模拟请求返回值
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metadata": {
  "basic": {
    "name": "2020-2023年青藏高原冰川监测数据集",
    "identifier": [
      {
        "id": "http://doi.org/10.1234/example",
        "type": "DOI"
      }
    ],
    "description": "本数据集包含青藏高原主要冰川的年度厚度变化监测数据本数据集包含青藏高原主要冰川的年度厚度变化监测数据本数据集包含青藏高原主要冰川的年度厚度变化监测数据本数据集包含青藏高原主要冰川的年度厚度变化监测数据本数据集包含青藏高原主要冰川的年度厚度变化监测数据...（不少于50字）",
    "keywords": [
      "冰川学",
      "遥感监测",
      "气候变化"
    ],
    "url": "https://example.com/dataset/123",
    "dateCreated": "2023/05/01",
    "subject": [
      "地球科学>冰川学"
    ],
    "format": ["netcdf", "tiff"],
    "image": "https://example.com/image.jpg",
  },
  "distribution": {
    "accessRights": "read",
    "license": "https://creativecommons.org/licenses/by/4.0/",
    "byteSize": 1024,
    "fileNumber": 10,
    "downloadURL": "http://example.com/dataset/123/download",
  },
  "rights": {
    "creator": [
      "中国科学院青藏高原研究所"
    ],
    "publisher": "国家冰川数据中心",
    "contactPoint": [
      "张研究员"
    ],
    "email": [
      "contact@example.com"
    ],
    "copyrightHolder": ["zhang"],
    "references": ["https://example.com/reference1", "https://example.com/reference2"]
  },
  "instrument": {
    "instrumentID": "GEO-001",
    "model": "GLACIER-2020",
    "name": "多光谱冰川监测雷达",
    "description": "采用X波段雷达测量冰川厚度...",
    "supportingInstitution": "中国科学院",
    "manufacturer": "中电科集团",
    "accountablePerson": "王主任",
    "contactPoint": "李工程师",
    "email": [
      "support@example.com"
    ]
  }
},
            "dataframeIds": ["/ds001/file001.nc", "/ds001/file002.csv", "ds001/file003.tif"],  ##文件完整路径
            "access_info": {
                "type": "local"
            }
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        service = MetaCatService()
        service.datasets = {"1": "Dataset1"}  # 模拟已有数据集
        result = service.list_dataset_files(token="", username="test_user", dataset_name="Dataset1")

        # 验证结果
        self.assertIsNotNone(result)
        self.assertEqual(result.meta.basic.keywords, ['冰川学', '遥感监测', '气候变化'])
        self.assertEqual(result.meta.basic.dateCreated.year, 2023)
        self.assertEqual(result.dataframeIds, ["/ds001/file001.nc", "/ds001/file002.csv", "ds001/file003.tif"])

    @patch("services.datasource.services.metacat_service.requests.get")
    def test_check_permission(self, mock_get):
        # 模拟请求返回值
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": {"result": True}}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        service = MetaCatService()
        result = service._check_permission(dataset_id="1", username="test_user")

        # 验证结果
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()