from odp.client.utils import convert_geometry


def test_geojson_str_to_wkb():
    data = '{"type": "Point", "coordinates": [125.6, 10.1]}'

    data = convert_geometry(data, "wkb")

    assert data == b"\x01\x01\x00\x00\x00ffffff_@333333$@"


def test_geojson_dict_to_wkb():
    data = {"type": "Point", "coordinates": [125.6, 10.1]}

    data = convert_geometry(data, "wkb")

    assert data == b"\x01\x01\x00\x00\x00ffffff_@333333$@"


def test_geojson_complex_dict_to_wkb():
    data = {"other_field": 1, "geo": {"type": "Point", "coordinates": [125.6, 10.1]}, "some_string": "text here"}

    data = convert_geometry(data, "wkb")

    assert data == {"other_field": 1, "geo": b"\x01\x01\x00\x00\x00ffffff_@333333$@", "some_string": "text here"}


def test_geojson_complex_dict_list_to_wkb():
    data = {
        "other_field": 1,
        "geo": [
            {"type": "Point", "coordinates": [125.6, 10.1]},
            {"type": "Point", "coordinates": [126.6, 10.1]},
            {"type": "Point", "coordinates": [127.6, 10.1]},
        ],
        "some_string": "text here",
    }

    data = convert_geometry(data, "wkb")

    assert data == {
        "other_field": 1,
        "geo": [
            b"\x01\x01\x00\x00\x00ffffff_@333333$@",
            b"\x01\x01\x00\x00\x00fffff\xa6_@333333$@",
            b"\x01\x01\x00\x00\x00fffff\xe6_@333333$@",
        ],
        "some_string": "text here",
    }


def test_geojson_single_list_to_wkb():
    data = [{"type": "Point", "coordinates": [125.6, 10.1]}]

    data = convert_geometry(data, "wkb")

    assert data[0] == b"\x01\x01\x00\x00\x00ffffff_@333333$@"


def test_geojson_multiple_list_to_wkb():
    data = [
        {"type": "Point", "coordinates": [125.6, 10.1]},
        {"type": "Point", "coordinates": [126.6, 10.1]},
        {"type": "Point", "coordinates": [127.6, 10.1]},
    ]

    data = convert_geometry(data, "wkb")

    assert data[0] == b"\x01\x01\x00\x00\x00ffffff_@333333$@"
    assert data[1] == b"\x01\x01\x00\x00\x00fffff\xa6_@333333$@"
    assert data[2] == b"\x01\x01\x00\x00\x00fffff\xe6_@333333$@"


def test_wkt_str_to_wkb():
    data = "POINT (125.6 10.1)"

    data = convert_geometry(data, "wkb")

    assert data == b"\x01\x01\x00\x00\x00ffffff_@333333$@"


def test_wkt_complex_dict_to_wkb():
    data = {"other_field": 1, "geo": "POINT (125.6 10.1)", "some_string": "text here"}

    data = convert_geometry(data, "wkb")

    assert data == {"other_field": 1, "geo": b"\x01\x01\x00\x00\x00ffffff_@333333$@", "some_string": "text here"}


def test_wkt_complex_dict_list_to_wkb():
    data = {
        "other_field": 1,
        "geo": ["POINT (125.6 10.1)", "POINT (126.6 10.1)", "POINT (127.6 10.1)"],
        "some_string": "text here",
    }

    data = convert_geometry(data, "wkb")

    assert data == {
        "other_field": 1,
        "geo": [
            b"\x01\x01\x00\x00\x00ffffff_@333333$@",
            b"\x01\x01\x00\x00\x00fffff\xa6_@333333$@",
            b"\x01\x01\x00\x00\x00fffff\xe6_@333333$@",
        ],
        "some_string": "text here",
    }


def test_wkt_single_list_to_wkb():
    data = ["POINT (125.6 10.1)"]

    data = convert_geometry(data, "wkb")

    assert data[0] == b"\x01\x01\x00\x00\x00ffffff_@333333$@"


def test_wkt_list_multiple_to_wkb():
    data = [
        "POINT (125.6 10.1)",
        "POINT (126.6 10.1)",
        "POINT (127.6 10.1)",
    ]

    data = convert_geometry(data, "wkb")

    assert data[0] == b"\x01\x01\x00\x00\x00ffffff_@333333$@"
    assert data[1] == b"\x01\x01\x00\x00\x00fffff\xa6_@333333$@"
    assert data[2] == b"\x01\x01\x00\x00\x00fffff\xe6_@333333$@"


def test_geojson_str_to_wkt():
    data = '{"type": "Point", "coordinates": [125.6, 10.1]}'

    data = convert_geometry(data, "wkt")

    assert data == "POINT (125.6 10.1)"


def test_geojson_dict_to_wkt():
    data = {"type": "Point", "coordinates": [125.6, 10.1]}

    data = convert_geometry(data, "wkt")

    assert data == "POINT (125.6 10.1)"


def test_geojson_complex_dict_to_wkt():
    data = {"other_field": 1, "geo": {"type": "Point", "coordinates": [125.6, 10.1]}, "some_string": "text here"}

    data = convert_geometry(data, "wkt")

    assert data == {"other_field": 1, "geo": "POINT (125.6 10.1)", "some_string": "text here"}


def test_geojson_complex_dict_list_to_wkt():
    data = {
        "other_field": 1,
        "geo": [
            {"type": "Point", "coordinates": [125.6, 10.1]},
            {"type": "Point", "coordinates": [126.6, 10.1]},
            {"type": "Point", "coordinates": [127.6, 10.1]},
        ],
        "some_string": "text here",
    }

    data = convert_geometry(data, "wkt")

    assert data == {
        "other_field": 1,
        "geo": ["POINT (125.6 10.1)", "POINT (126.6 10.1)", "POINT (127.6 10.1)"],
        "some_string": "text here",
    }


def test_geojson_single_list_to_wkt():
    data = [{"type": "Point", "coordinates": [125.6, 10.1]}]

    data = convert_geometry(data, "wkt")

    assert data[0] == "POINT (125.6 10.1)"


def test_geojson_multiple_list_to_wkt():
    data = [
        {"type": "Point", "coordinates": [125.6, 10.1]},
        {"type": "Point", "coordinates": [126.6, 10.1]},
        {"type": "Point", "coordinates": [127.6, 10.1]},
    ]

    data = convert_geometry(data, "wkt")

    assert data[0] == "POINT (125.6 10.1)"
    assert data[1] == "POINT (126.6 10.1)"
    assert data[2] == "POINT (127.6 10.1)"


def test_wkb_str_to_wkt():
    data = "\x01\x01\x00\x00\x00ffffff_@333333$@"

    data = convert_geometry(data, "wkt", 1)

    assert data == "POINT (125.6 10.1)"


def test_wkb_bytes_to_wkt():
    data = b"\x01\x01\x00\x00\x00ffffff_@333333$@"

    data = convert_geometry(data, "wkt", 1)

    assert data == "POINT (125.6 10.1)"


def test_wkb_complex_dict_to_wkt():
    data = {"other_field": 1, "geo": b"\x01\x01\x00\x00\x00ffffff_@333333$@", "some_string": "text here"}

    data = convert_geometry(data, "wkt", 1)

    assert data == {"other_field": 1, "geo": "POINT (125.6 10.1)", "some_string": "text here"}


def test_wkb_complex_dict_list_to_wkt():
    data = {
        "other_field": 1,
        "geo": [
            b"\x01\x01\x00\x00\x00ffffff_@333333$@",
            b"\x01\x01\x00\x00\x00fffff\xa6_@333333$@",
            b"\x01\x01\x00\x00\x00fffff\xe6_@333333$@",
        ],
        "some_string": "text here",
    }

    data = convert_geometry(data, "wkt", 1)

    assert data == {
        "other_field": 1,
        "geo": ["POINT (125.6 10.1)", "POINT (126.6 10.1)", "POINT (127.6 10.1)"],
        "some_string": "text here",
    }


def test_wkb_single_list_to_wkt():
    data = [b"\x01\x01\x00\x00\x00ffffff_@333333$@"]

    data = convert_geometry(data, "wkt", 1)

    assert data[0] == "POINT (125.6 10.1)"


def test_wkb_list_multiple_to_wkt():
    data = [
        b"\x01\x01\x00\x00\x00ffffff_@333333$@",
        b"\x01\x01\x00\x00\x00fffff\xa6_@333333$@",
        b"\x01\x01\x00\x00\x00fffff\xe6_@333333$@",
    ]

    data = convert_geometry(data, "wkt", 1)

    assert data[0] == "POINT (125.6 10.1)"
    assert data[1] == "POINT (126.6 10.1)"
    assert data[2] == "POINT (127.6 10.1)"


def test_wkt_str_to_geojson():
    data = "POINT (125.6 10.1)"

    data = convert_geometry(data, "geojson")

    assert data == {"type": "Point", "coordinates": [125.6, 10.1]}


def test_wkt_complex_dict_to_geojson():
    data = {"other_field": 1, "geo": "POINT (125.6 10.1)", "some_string": "text here"}

    data = convert_geometry(data, "geojson")

    assert data == {
        "other_field": 1,
        "geo": {"type": "Point", "coordinates": [125.6, 10.1]},
        "some_string": "text here",
    }


def test_wkt_complex_dict_list_to_geojson():
    data = {
        "other_field": 1,
        "geo": ["POINT (125.6 10.1)", "POINT (126.6 10.1)", "POINT (127.6 10.1)"],
        "some_string": "text here",
    }

    data = convert_geometry(data, "geojson")

    assert data == {
        "other_field": 1,
        "geo": [
            {"type": "Point", "coordinates": [125.6, 10.1]},
            {"type": "Point", "coordinates": [126.6, 10.1]},
            {"type": "Point", "coordinates": [127.6, 10.1]},
        ],
        "some_string": "text here",
    }


def test_wkt_single_list_to_geojson():
    data = ["POINT (125.6 10.1)"]

    data = convert_geometry(data, "geojson")

    assert data[0] == {"type": "Point", "coordinates": [125.6, 10.1]}


def test_wkt_list_multiple_to_geojson():
    data = [
        "POINT (125.6 10.1)",
        "POINT (126.6 10.1)",
        "POINT (127.6 10.1)",
    ]

    data = convert_geometry(data, "geojson")

    assert data[0] == {"type": "Point", "coordinates": [125.6, 10.1]}
    assert data[1] == {"type": "Point", "coordinates": [126.6, 10.1]}
    assert data[2] == {"type": "Point", "coordinates": [127.6, 10.1]}


def test_wkb_str_to_geojson():
    data = "\x01\x01\x00\x00\x00ffffff_@333333$@"

    data = convert_geometry(data, "geojson")

    assert data == {"type": "Point", "coordinates": [125.6, 10.1]}


def test_wkb_bytes_to_geojson():
    data = b"\x01\x01\x00\x00\x00ffffff_@333333$@"

    data = convert_geometry(data, "geojson")

    assert data == {"type": "Point", "coordinates": [125.6, 10.1]}


def test_wkb_complex_dict_to_geojson():
    data = {"other_field": 1, "geo": b"\x01\x01\x00\x00\x00ffffff_@333333$@", "some_string": "text here"}

    data = convert_geometry(data, "geojson")

    assert data == {
        "other_field": 1,
        "geo": {"type": "Point", "coordinates": [125.6, 10.1]},
        "some_string": "text here",
    }


def test_wkb_complex_dict_list_to_geojson():
    data = {
        "other_field": 1,
        "geo": [
            b"\x01\x01\x00\x00\x00ffffff_@333333$@",
            b"\x01\x01\x00\x00\x00fffff\xa6_@333333$@",
            b"\x01\x01\x00\x00\x00fffff\xe6_@333333$@",
        ],
        "some_string": "text here",
    }

    data = convert_geometry(data, "geojson")

    assert data == {
        "other_field": 1,
        "geo": [
            {"type": "Point", "coordinates": [125.6, 10.1]},
            {"type": "Point", "coordinates": [126.6, 10.1]},
            {"type": "Point", "coordinates": [127.6, 10.1]},
        ],
        "some_string": "text here",
    }


def test_wkb_single_list_to_geojson():
    data = [b"\x01\x01\x00\x00\x00ffffff_@333333$@"]

    data = convert_geometry(data, "geojson")

    assert data[0] == {"type": "Point", "coordinates": [125.6, 10.1]}


def test_wkb_list_multiple_to_geojson():
    data = [
        b"\x01\x01\x00\x00\x00ffffff_@333333$@",
        b"\x01\x01\x00\x00\x00fffff\xa6_@333333$@",
        b"\x01\x01\x00\x00\x00fffff\xe6_@333333$@",
    ]

    data = convert_geometry(data, "geojson")

    assert data[0] == {"type": "Point", "coordinates": [125.6, 10.1]}
    assert data[1] == {"type": "Point", "coordinates": [126.6, 10.1]}
    assert data[2] == {"type": "Point", "coordinates": [127.6, 10.1]}
