import re
from datetime import date
from typing import List, Dict, Optional, Union
from pydantic import BaseModel, HttpUrl, Field, field_validator

class Identifier(BaseModel):
    """唯一标识符（多值字典结构）"""
    id: HttpUrl  # 符合URI格式的标识，如DOI或CSTR
    type: str = Field(..., pattern="^(DOI|CSTR|InternalID)$")  # 标识类型约束

class BasicInfo(BaseModel):
    """基本信息"""
    name: str = Field(..., min_length=10)
    identifier: List[Identifier]  # 多值字典
    description: str = Field(..., min_length=20)
    keywords: List[str] = Field(..., min_items=3)
    url: HttpUrl
    datePublished: date
    subject: List[str] = Field(..., max_items=3)
    format: Optional[List[str]] = None  # 可选字段
    image: Optional[HttpUrl] = None     # 可选字段

class AccessRights(BaseModel):
    """访问权限信息"""
    type: str  # 如 "open"
    openDate: Optional[date] = None
    condition: Optional[str] = None

class DistributionInfo(BaseModel):
    """分发信息"""
    accessRights: Optional[AccessRights] = None
    license: str
    byteSize: Optional[float] = Field(None, description="存储量（单位：KB）")
    fileNumber: Optional[int] = None
    downloadURL: Optional[HttpUrl] = None

class RightsInfo(BaseModel):
    """权益信息"""
    creator: List[str]
    publisher: str
    contactPoint: List[str]
    email: List[str]
    copyrightHolder: Optional[List[str]] = None
    references: Optional[List[HttpUrl]] = None

    # 邮箱格式验证
    @field_validator('email')
    @classmethod
    def validate_emails(cls, v):
        email_pattern = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
        for email in v:
            if not email_pattern.match(email):
                raise ValueError(f"Invalid email format: {email}")
        return v

class InstrumentInfo(BaseModel):
    """装置信息"""
    instrumentID: str
    model: str
    name: str
    description: str
    supportingInstitution: str
    manufacuturer: str
    accountablePerson: str
    contactPoint: str
    email: List[str]

    # 邮箱格式验证
    @field_validator('email')
    @classmethod
    def validate_emails(cls, v):
        email_pattern = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
        for email in v:
            if not email_pattern.match(email):
                raise ValueError(f"Invalid email format: {email}")
        return v

# -------------------元数据模型 -------------------
class DatasetMetadata(BaseModel):
    basic: BasicInfo
    distribution: DistributionInfo
    rights: RightsInfo
    instrument: InstrumentInfo


# ------------------- 使用示例 -------------------
# if __name__ == "__main__":
#     example = DatasetMetadata(
#         basic=BasicInfo(
#             name="2020-2023年青藏高原冰川监测数据集",
#             identifier=[Identifier(id="http://doi.org/10.1234/example", type="DOI")],
#             description="本数据集包含青藏高原主要冰川的年度厚度变化监测数据本数据集包含青藏高原主要冰川的年度厚度变化监测数据本数据集包含青藏高原主要冰川的年度厚度变化监测数据本数据集包含青藏高原主要冰川的年度厚度变化监测数据本数据集包含青藏高原主要冰川的年度厚度变化监测数据...（不少于50字）",
#             keywords=["冰川学", "遥感监测", "气候变化"],
#             url="https://example.com/dataset/123",
#             datePublished=date(2023, 5, 1),
#             subject=["地球科学>冰川学"]
#         ),
#         distribution=DistributionInfo(
#             license="https://creativecommons.org/licenses/by/4.0/"
#         ),
#         rights=RightsInfo(
#             creator=["中国科学院青藏高原研究所"],
#             publisher="国家冰川数据中心",
#             contactPoint=["张研究员"],
#             email=["contact@example.com"]
#         ),
#         instrument=InstrumentInfo(
#             instrumentID="GEO-001",
#             model="GLACIER-2020",
#             name="多光谱冰川监测雷达",
#             description="采用X波段雷达测量冰川厚度...",
#             supportingInstitution="中国科学院",
#             manufacturer="中电科集团",
#             accountablePerson="王主任",
#             contactPoint="李工程师",
#             email=["support@example.com"]
#         )
#     )
#     print(example.model_dump_json(indent=2))