from typing import Optional, List

from pydantic import BaseModel

from odp_sdk.dto.schema import Schema
from odp_sdk.dto.tabular_store import TablePartitioningSpec


class TableSpec(BaseModel):
    table_schema: Schema
    partitioning: Optional[List[TablePartitioningSpec]] = None