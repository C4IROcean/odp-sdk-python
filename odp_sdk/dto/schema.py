from typing import Callable, Dict, FrozenSet, Iterable, Tuple, Union

import pyarrow as pa
from pydantic import BaseModel, Extra, Field, PrivateAttr, root_validator, validator

_SIMPLE_DATATYPES = {
    "bool": pa.bool_,
    "int": pa.int32,
    "int32": pa.int32,
    "long": pa.int64,
    "int64": pa.int64,
    "double": pa.float64,
    "date": pa.date64,
    "date64": pa.date64,
    "string": pa.string,
    "binary": pa.binary,
    "geometry": pa.binary,
}

_COMPLEX_TYPES = {
    "time": pa.time64,
    "time64": pa.time64,
    "timestamp": pa.timestamp,
    "list": pa.list_,
    "map": pa.map_,
}

_GEOMETRY_REPRESENTATION = ["wkt", "wkb", "geojson"]
_DATA_TYPES_SUPPORTED_IN_COMPLEX_TYPES = ["bool", "int", "int32", "long", "int64", "double", "string", "binary"]


class Column(BaseModel):
    """Column class.

    >>> Column(type="string", metadata={"type":"generic names"}, nullable=True)
    """

    type: str
    """Type of a column."""

    metadata: Dict[str, str] = Field(default_factory=dict)
    """Metadata about the column."""

    nullable: bool = True
    """Flag to indicate if the attribute is nullable."""

    _internal_type: Callable = PrivateAttr(None)
    _type_without_parameters: str = PrivateAttr(None)
    _internal_type_args: Union[str, int] = PrivateAttr(None)

    def __init__(self, **kwargs):
        """Initialise Column."""
        super().__init__(**kwargs)

        self._parse_type()

    @root_validator
    def _validate_metadata(cls, values):
        if (
            values.get("type") == "geometry"
            and values["metadata"].get("representation", "geojson") not in _GEOMETRY_REPRESENTATION
        ):
            raise ValueError("representation must be either 'wkt' or 'wkb' or 'geojson'")

        return values

    @validator("type")
    def validate_type(cls, value):
        """Validate the user defined type.

        Args:
            value (str): Value of the current field.

        Raises:
            ValueError: if any validations are failed.

        Returns:
            str: Input value as is.
        """
        if value not in _SIMPLE_DATATYPES and "<" in value:
            type_info = value.split("<")[1].strip(">").split(",")
            if value.startswith("timestamp"):
                if type_info[0] not in ("s", "ms", "us", "ns"):
                    raise ValueError("Invalid timestamp type")

            elif value.startswith("time"):
                if type_info[0] not in ("us", "ns"):
                    raise ValueError("Invalid time type")

            elif value.startswith("date"):
                if type_info[0] not in ("ms"):
                    raise ValueError("Invalid date type")

            elif value.startswith("list"):
                if type_info[0].replace("item: ", "") not in _DATA_TYPES_SUPPORTED_IN_COMPLEX_TYPES:
                    raise ValueError("Invalid list type")

            elif value.startswith("map"):
                if (
                    type_info[0].strip(" ") not in _DATA_TYPES_SUPPORTED_IN_COMPLEX_TYPES
                    or type_info[1].strip(" ") not in _DATA_TYPES_SUPPORTED_IN_COMPLEX_TYPES
                ):
                    raise ValueError("Invalid map type")
            else:
                raise ValueError("Invalid type.")

        elif value not in _SIMPLE_DATATYPES:
            raise ValueError("Invalid type.")

        # Return original value
        return value

    def _parse_type(self):
        """Parse the user type.

        Method to populate the internal type fields.
        """
        self._type_without_parameters = self.type
        if self.type not in _SIMPLE_DATATYPES and "<" in self.type:
            col_type, type_info = self.type.split("<")
            type_info = type_info.strip(">").split(",")

            if self.type.startswith("timestamp"):
                self._type_without_parameters = "timestamp"
                complex_type = _COMPLEX_TYPES[col_type], (*type_info,)

            elif self.type.startswith("time"):
                self._type_without_parameters = "time"
                complex_type = _COMPLEX_TYPES[col_type], (type_info[0],)

            elif self.type.startswith("date"):
                self._type_without_parameters = "date"
                complex_type = _SIMPLE_DATATYPES[col_type], ()

            elif self.type.startswith("list"):
                self._type_without_parameters = "list"
                int_type = type_info[0].replace("item: ", "")
                complex_type = _COMPLEX_TYPES[col_type], (_SIMPLE_DATATYPES[int_type],)

            elif self.type.startswith("map"):
                self._type_without_parameters = "map"
                complex_type = _COMPLEX_TYPES[col_type], (
                    _SIMPLE_DATATYPES[type_info[0].strip(" ")],
                    _SIMPLE_DATATYPES[type_info[1].strip(" ")],
                )

            self._internal_type, self._internal_type_args = complex_type

        else:
            self._internal_type = _SIMPLE_DATATYPES[self.type]

    @property
    def internal_type(self):
        """Get the internal type."""
        return self._internal_type

    @property
    def type_without_parameters(self):
        """Get the user defined type without any parameters."""
        return self._type_without_parameters

    @property
    def internal_type_args(self):
        """Get the arguments for an internal type."""
        return self._internal_type_args

class Schema(BaseModel):
    """Schema class."""

    _internal_table_schema: pa.Schema = PrivateAttr(None)
    _cast_column_names: Dict = PrivateAttr(default_factory=dict)
    _table_schema: Dict[str, Column] = PrivateAttr(None)

    class Config:
        extra = Extra.allow

    def __init__(self, **kwargs: Dict):
        """Initialise Schema."""

        kwargs = {key: Column(**value) for key, value in kwargs.items()}

        super().__init__(**kwargs)

        self._table_schema = kwargs
        self._populate_internal_schema()
        self._extract_cast_column_names()

    def __getitem__(self, item):
        return getattr(self, item)

    def items(self) -> Iterable[Tuple[str, Column]]:
        return self._table_schema.items()

    def _populate_internal_schema(self) -> pa.Schema:
        pa_schema = []

        for column in self._table_schema:
            column_info = self._table_schema[column]
            if not column_info.internal_type_args:
                col_type = pa.field(column, column_info.internal_type(), metadata=column_info.metadata)
            else:
                # Parse and construct the custom arguments to construct a pyarrow internal type.
                custom_args = []
                for args in column_info.internal_type_args:
                    if callable(args):
                        custom_args.append(args())
                    else:
                        custom_args.append(args)

                col_type = pa.field(
                    column,
                    column_info.internal_type(*custom_args),
                    metadata=column_info.metadata,
                )
            pa_schema.append(col_type)
        self._internal_table_schema = pa.schema(pa_schema)

    def _extract_cast_column_names(self) -> None:
        for column in self._table_schema:
            self._cast_column_names.setdefault(self._table_schema[column].type_without_parameters, []).append(column)

    def get_internal_schema(self) -> pa.Schema:
        """Get internal schema for a table.

        Returns:
            pa.Schema: Internal schema for a table.
        """
        return self._internal_table_schema

    def get_schema(self) -> Dict:
        """Get user defined schema for a table.

        Returns:
            Dict: Open table schema for a table.
        """
        return self._table_schema

    def get_field_names(self) -> FrozenSet:
        """Get field names.

        Returns:
            FrozenSet: Get field names as Frozenset.
        """
        return frozenset(self._table_schema)

    @property
    def geometry_columns(self):
        """Get geometry type column names."""
        return self._cast_column_names.get("geometry", [])

    @property
    def date_columns(self):
        """Get date type column names."""
        return self._cast_column_names.get("date", []) + self._cast_column_names.get("date64", [])

    @property
    def time_columns(self):
        """Get time type column names."""
        return self._cast_column_names.get("time", []) + self._cast_column_names.get("time64", [])

    @property
    def timestamp_columns(self):
        """Get timestamp type column names."""
        return self._cast_column_names.get("timestamp", [])
