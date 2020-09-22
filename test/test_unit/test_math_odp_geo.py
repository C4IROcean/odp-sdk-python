#  Copyright 2020 Ocean Data Foundation AS
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import unittest

from odp_sdk.math import index_to_gcs, gcs_to_index
import numpy as np


class TestOdpMath(unittest.TestCase):

    def test_gcs2index(self):

        geo_arr = [
            [-2, -179, 88],
            [-89, -179, 1],
            [90, -179, 180],
            [0, 0, 32310],
            [90, 180, 64800],
            [-67, -179, 23],
            [-67.1, -179.0, 23],
            [-67.3, -179.1, 23]
        ]

        for lat, lon, ref_index in geo_arr:

            index = gcs_to_index(lat, lon)

            self.assertEqual(index, ref_index)

    def test_gcs2index_list(self):

        lat = [-2, -89, 90, 0, 90, -67, -67.1, -67.3]
        lon = [-179, -179, -179, 0, 180, -179, -179.0, -179.1]
        ref_index = [88, 1, 180, 32310, 64800, 23, 23, 23]

        index = gcs_to_index(lat, lon)

        self.assertListEqual(index.tolist(), ref_index)

    def test_gcs2index_np_array(self):

        lat = np.array([-2, -89, 90, 0, 90, -67, -67.1, -67.3])
        lon = np.array([-179, -179, -179, 0, 180, -179, -179.0, -179.1])
        ref_index = np.array([88, 1, 180, 32310, 64800, 23, 23, 23])

        index = gcs_to_index(lat, lon)

        np.testing.assert_array_equal(index, ref_index)

    def test_index2gcs(self):

        geo_arr = [
            [-2, -179, 88],
            [-89, -179, 1],
            [90, -179, 180],
            [0, 0, 32310],
            [90, 180, 64800],
            [-67, -179, 23]
        ]

        for ref_lat, ref_lon, index in geo_arr:

            lat, lon = index_to_gcs(index)

            self.assertEqual(lat, ref_lat)
            self.assertEqual(lon, ref_lon)
