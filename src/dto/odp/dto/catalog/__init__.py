from odp.dto.resource import ResourceDto

from .data_collection import DataCollectionSpec, Distribution
from .dataset import Attribute, Citation, DatasetSpec
from .observable import ObservableSpec

# Convenience type aliases
DataCollectionDto = ResourceDto[DataCollectionSpec]
DatasetDto = ResourceDto[DatasetSpec]
ObservableDto = ResourceDto[ObservableSpec]

del ResourceDto
