# -*- coding: utf-8 -*-
"""Geographical coordinate transformations, used to convert
between lat/lon, ODP-index and NETCDF4 grid-space
"""

import numpy as np


def gcs_to_index(lat, lon, res=1):
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


def gcs_to_grid(lat, lon, res=1):
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


def grid_to_index(x, y, res=1):
    """Convert grid-coordinates to ODP-Index

    Args:
        x: lon, range with res = 1, 1 -> 360
        y: lat, range with res = 1, 1 -> 360
        res: Resolution where 1x1 degress per index is default.
            For half-degree grids, use 0.5

    Returns:
        (int): ODP-index
    """

    return np.cast[np.int](
        (x - 1) * 180 / res + y
    )


def index_to_grid(index, res=1):
    """Convert ODP-index to grid-coordinates

    Args:
        index: ODP-index in the range [1, 64800] when res=1
        res: Resolution where 1x1 degress per index is default.
            For half-degree grids, use 0.5

    Returns:
        tuple(int, int): Grid-coordinates
    """

    lat_range = np.int(np.round(res * 180.0))

    xloc = (index - 1) // lat_range + 1
    yloc = (index - 1) % lat_range + 1

    return xloc, yloc


def index_to_gcs(index, res=1):
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


if __name__ == '__main__':
    geo_arr = [[-2,-180,88], [-89,-179, 1],[90,-179, 180],[0,0,32310],[90,180,64800], [-67,-179,23], ]

    for lat, lon, ref_index in geo_arr:

        index = gcs_to_index(lat, lon, 1)
        print(ref_index, index)
        assert index == ref_index

    lat = np.array([x for x, _, _ in geo_arr])
    lon = np.array([x for _, x, _ in geo_arr])
    ref_index = np.array([x for _, _, x in geo_arr])

    index = gcs_to_index(lat, lon)
    print(np.equal(index, ref_index))

