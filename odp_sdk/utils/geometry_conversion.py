import json
from typing import Optional, Union

import geojson
from shapely import wkb, wkt
from shapely.geometry import shape


def convert_geometry(
    data: Union[str, dict, list, bytes], result_geometry: str, rounding_precision: Optional[int] = None
):
    if result_geometry == "wkb":
        return _convert_geometry_to_wkb(data)
    elif result_geometry == "wkt":
        return _convert_geometry_to_wkt(data, rounding_precision)
    elif result_geometry == "geojson":
        if _is_geojson(data):
            return data
        return _convert_geometry_to_geojson(data)


def _convert_geometry_to_wkb(data: Union[str, dict, list]):
    if _is_geojson(data):
        return _convert_geojson_to_wkb(data)
    if isinstance(data, str):
        try:
            return _convert_wkt_to_wkb(data)
        except Exception:
            pass
    elif isinstance(data, dict):
        for key in data:
            value = data[key]
            data[key] = _convert_geometry_to_wkb(value)
    elif isinstance(data, list):
        for i, row in enumerate(data):
            try:
                data[i] = _convert_geometry_to_wkb(row)
            except Exception:
                continue
    return data


def _convert_geometry_to_wkt(data: Union[str, dict, list, bytes], rounding_precision: Optional[int] = None):
    if _is_geojson(data):
        return _convert_geojson_to_wkt(data)
    if isinstance(data, (str, bytes)):
        try:
            return _convert_wkb_to_wkt(data, rounding_precision)
        except Exception:
            pass
    elif isinstance(data, dict):
        for key in data:
            value = data[key]
            data[key] = _convert_geometry_to_wkt(value, rounding_precision)
    elif isinstance(data, list):
        for i, row in enumerate(data):
            try:
                data[i] = _convert_geometry_to_wkt(row, rounding_precision)
            except Exception:
                continue
    return data


def _convert_geometry_to_geojson(data: Union[str, dict, list, bytes]):
    if isinstance(data, str):
        try:
            if _is_wkt(data):
                return _convert_wkt_to_geojson(data)
            else:
                return _convert_wkb_to_geojson(data)
        except Exception:
            pass
    elif isinstance(data, bytes):
        try:
            return _convert_wkb_to_geojson(data)
        except Exception:
            pass
    elif isinstance(data, dict):
        for key in data:
            value = data[key]
            data[key] = _convert_geometry_to_geojson(value)
    elif isinstance(data, list):
        for i, row in enumerate(data):
            try:
                data[i] = _convert_geometry_to_geojson(row)
            except Exception:
                continue
    return data


def _convert_geojson_to_wkb(geojson_data: Union[dict, str]) -> bytes:
    if isinstance(geojson_data, dict):
        geojson_data = json.dumps(geojson_data)
    geo = geojson.loads(geojson_data)
    return shape(geo).wkb


def _convert_geojson_to_wkt(geojson_data: Union[dict, str]) -> str:
    if isinstance(geojson_data, dict):
        geojson_data = json.dumps(geojson_data)
    geo = geojson.loads(geojson_data)
    return shape(geo).wkt


def _convert_wkb_to_geojson(wkb_data: Union[bytes, str]) -> dict:
    geo = wkb.loads(wkb_data)
    return geojson.Feature(geometry=geo, properties={}).geometry


def _convert_wkb_to_wkt(wkb_data: Union[bytes, str], rounding_precision: Optional[int] = None) -> str:
    if rounding_precision:
        return wkt.dumps(wkb.loads(wkb_data), rounding_precision=rounding_precision)
    return wkt.dumps(wkb.loads(wkb_data))


def _convert_wkt_to_geojson(wkt_data: str) -> dict:
    geo = wkt.loads(wkt_data)
    return geojson.Feature(geometry=geo, properties={}).geometry


def _convert_wkt_to_wkb(wkt_data: str) -> bytes:
    return wkb.dumps(wkt.loads(wkt_data))


def _is_geojson(data) -> bool:
    if isinstance(data, dict):
        return len(data.keys()) == 2 and "type" in data and "coordinates" in data
    elif isinstance(data, str):
        try:
            return _is_geojson(json.loads(data))
        except Exception:
            return False
    return False


def _is_wkt(data: str) -> bool:
    # Cheap way of checking if the value is a WKT string
    #   Simply see if the first character is the first letter of a WKT-Object:
    #     P: Point, Polygon
    #     L: LineString
    #     M: MultiPoint, MultiPolygon, MultiLineString
    #     G: GeometryCollection
    return data[0].upper() in {"P", "L", "M", "G"}
