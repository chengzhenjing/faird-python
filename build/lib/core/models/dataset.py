# from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional
from core.models.dataset_meta import DatasetMetadata

@dataclass
class DataSet:
    meta: Optional[DatasetMetadata]
    dataframeIds : List["str"] = field(default_factory=list)
    accessible: bool = False
    accessInfo: Optional["dict"] = None