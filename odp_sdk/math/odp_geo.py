# -*- coding: utf-8 -*-
"""Geographical coordinate transformations, used to convert
between lat/lon, ODP-index and NETCDF4 grid-space
"""

import numpy as np

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

GRID_X_WIDTH = 360
GRID_Y_WIDTH = 180

INDEX_COUNT = GRID_X_WIDTH * GRID_Y_WIDTH


def gcs_to_index(
        lat: Union[float, List[float], np.ndarray],
        lon: Union[float, List[float], np.ndarray],
        res: float = 1.0
) -> np.ndarray:
    """Convert lat/lon to ODP index

    Args:
        lat: Latitude
        lon: Longitude
        res: Resolution where 1x1 degress per index is default.
            For half-degree grids, use 0.5

    Returns:
        float: ODP-index

    """

    lat = np.maximum(lat, -89)
    lon = np.maximum(lon, -179)

    x, y = gcs_to_grid(lat, lon, res)
    return grid_to_index(x, y, res)


def gcs_to_grid(
        lat: Union[float, List[float], np.ndarray],
        lon: Union[float, List[float], np.ndarray],
        res=1.0
) -> Union[Tuple[int, int], np.ndarray]:
    """Convert lat/lon to grid

    Args:
        lat: Latitude
        lon: Longitude
        res: Resolution where 1x1 degress per index is default.
            For half-degree grids, use 0.5

    Returns:
        tuple(int, int): Grid index
    """

    lat += 90
    lon += 180

    latr = res * np.ceil(lat / res)
    lonr = res * np.ceil(lon / res)

    y = latr / res
    x = lonr / res

    return np.cast[np.int](x), np.cast[np.int](y)


def grid_to_index(
        x: Union[int, float, List[Union[int, float]], np.ndarray],
        y: Union[int, float, List[Union[int, float]], np.ndarray],
        res: float = 1.0
) -> Union[int, np.ndarray]:
    """Convert grid-coordinates to ODP-Index

    Args:
        x: lon, range with res = 1, 1 -> 360
        y: lat, range with res = 1, 1 -> 360
        res: Resolution where 1x1 degress per index is default.
            For half-degree grids, use 0.5

    Returns:
        (int): ODP-index
    """

    ret = np.cast[np.int](
        (x - 1) * 180 / res + y
    )

    # Compensate for negative indices

    ret[ret < 0] += INDEX_COUNT

    return ret


def index_to_grid(
        index: Union[int, List[int], np.ndarray],
        res: float = 1.0
) -> Union[Tuple[int, int], np.ndarray]:
    """Convert ODP-index to grid-coordinates

    Args:
        index: ODP-index in the range [1, 64800] when res=1
        res: Resolution where 1x1 degress per index is default.
            For half-degree grids, use 0.5

    Returns:
        tuple(int, int): Grid-coordinates
    """

    lat_range = np.int(np.round(res * 180.0))

    xloc = np.subtract(index, 1) // lat_range + 1
    yloc = np.subtract(index, 1) % lat_range + 1

    return xloc, yloc


def index_to_gcs(
        index: Union[int, List[int], np.ndarray],
        res: float = 1.0
) -> Union[Tuple[float, float], np.ndarray]:
    """Convert ODP-index to lat/lon

    Args:
        index: ODP-index in the range [1, 64800] when res=1
        res: Resolution where 1x1 degress per index is default.
            For half-degree grids, use 0.5

    Returns:
        tuple(float, float): longitude, latitude
    """

    x, y = index_to_grid(index, res)
    lon = -180 + x / res
    lat = -90 + y / res

    return lat, lon


def grid_rect_members(
        p1: Tuple[int, int],
        p2: Tuple[int, int],
        compensate_dateline: bool = False
) -> np.ndarray:
    """Fill a rectangle, defined by two corner grid-coordinates, with all grid-coordinates contained in it

    Args:
        p1: First corner of rectangle
        p2: Second corner of rectangle
        compensate_dateline: Compensate for international dateline.
            If true, then two points close to each other near the international dateline or south pole
            will define a rectangle across the dateline, instead going all the way around the globe

    Returns:
        np.array: 2D-array of all grid-coordinates contained within the rectangle.

    Note:
        The ends are included. For example - if p1 and p2 are equal, then the returned array is NOT empty,
        but instead contains a single point - p1

    """

    x1, y1 = p1
    x2, y2 = p2

    # Retrieve SW and NE corners

    x1, x2 = min(x1, x2), max(x1, x2)
    y1, y2 = min(y1, y2), max(y1, y2)

    # Swap around international date-line

    if compensate_dateline and (x1 - x2 + 360 < x2 - x1):
        x1, x2 = x2, x1 + 360

    # Swap around south-pole

    if compensate_dateline and (y1 - y2 + 180 < y2 - y1):
        y1, y2 = y2, y1 + 180

    w = x2 - x1 + 1

    ret = np.zeros(((x2 - x1 + 1) * (y2 - y1 + 1), 2), dtype=int)

    for j in np.arange(y2 - y1 + 1):
        for i in np.arange(x2 - x1 + 1):
            ret[j * w + i, :] = (i + x1) % 360, (j + y1) % 180

    return ret


def index_rect_members(
        p1: int,
        p2: int,
        res: float = 1,
        compensate_dateline: bool = False
) -> np.array:
    """Fill a rectangle, defined by two corner geo-indices, with all geo-indices contained in it

    Args:
        p1: Geo-Index of first corner of rectangle
        p2: Geo-Index of second corner of rectangle
        res: Resolution where 1x1 degress per index is default.
            For half-degree grids, use 0.5
        compensate_dateline: Compensate for international dateline.
            If true, then two points close to each other near the international dateline or south pole
            will define a rectangle across the dateline, instead going all the way around the globe

    Returns:
        np.array: 1D-array of all geo-indices contained within the rectangle.

    Note:
        The ends are included. For example - if p1 and p2 are equal, then the returned array is NOT empty,
        but instead contains a single point - p1
    """

    x, y = index_to_grid([p1, p2])

    pg1 = x[0], y[0]
    pg2 = x[1], y[1]

    members = grid_rect_members(pg1, pg2, compensate_dateline)

    return grid_to_index(members[:, 0], members[:, 1], res=res)
