from textwrap import dedent

from odp_sdk.utils.ndjson import NdJsonParser


def test_parse_ndjson_simple():
    test_str = dedent(
        """
        {"name": "Alice", "age": 30}
        {"name": "Bob", "age": 25}
        {"name": "Charlie", "age": 35}
        """
    ).strip()

    ndjson_parser = NdJsonParser(s=test_str)

    parsed_rows = list(iter(ndjson_parser))

    assert isinstance(parsed_rows, list)
    assert len(parsed_rows) == 3

    assert parsed_rows[0]["name"] == "Alice"
    assert parsed_rows[0]["age"] == 30
    assert parsed_rows[1]["name"] == "Bob"
    assert parsed_rows[1]["age"] == 25
    assert parsed_rows[2]["name"] == "Charlie"
    assert parsed_rows[2]["age"] == 35


def test_parse_ndjson_binary_simple():
    test_str = (
        dedent(
            """
            {"product_id": 1, "name": "Widget", "price": 10.99}
            {"product_id": 2, "name": "Gadget", "price": 19.99}
            {"product_id": 3, "name": "Tool", "price": 15.49}
            """
        )
        .strip()
        .encode("utf-8")
    )

    ndjson_parser = NdJsonParser(s=test_str)

    parsed_rows = list(iter(ndjson_parser))

    assert isinstance(parsed_rows, list)
    assert len(parsed_rows) == 3

    assert parsed_rows[0]["product_id"] == 1
    assert parsed_rows[0]["name"] == "Widget"
    assert parsed_rows[0]["price"] == 10.99
    assert parsed_rows[1]["product_id"] == 2
    assert parsed_rows[1]["name"] == "Gadget"
    assert parsed_rows[1]["price"] == 19.99
    assert parsed_rows[2]["product_id"] == 3
    assert parsed_rows[2]["name"] == "Tool"
    assert parsed_rows[2]["price"] == 15.49


def test_parse_ndjson_special_characters():
    test_str = dedent(
        """
        {"fruits": ["apple", "banana", "cherry"], "description": "Delicious & healthy 🍏🍌🍒"}
        {"colors": ["red", "green", "blue"], "symbols": ["@#$%^&*()_+!"]}
        {"languages": ["English", "Español", "Français"], "special_chars": "ñçüëł"}
        """
    ).strip()

    ndjson_parser = NdJsonParser(s=test_str)
    parsed_rows = list(iter(ndjson_parser))

    assert isinstance(parsed_rows, list)
    assert len(parsed_rows) == 3

    assert parsed_rows[0]["fruits"] == ["apple", "banana", "cherry"]
    assert parsed_rows[0]["description"] == "Delicious & healthy 🍏🍌🍒"
    assert parsed_rows[1]["colors"] == ["red", "green", "blue"]
    assert parsed_rows[1]["symbols"] == ["@#$%^&*()_+!"]
    assert parsed_rows[2]["languages"] == ["English", "Español", "Français"]
    assert parsed_rows[2]["special_chars"] == "ñçüëł"


def test_parse_ndjson_embedded_json():
    test_str = dedent(
        """
        {"content": "Nested objects: {\\\"key1\\\": \\\"value1\\\", \\\"key2\\\": \\\"value2\\\"}"}
        {"config": "{ \\\"param1\\\": [1, 2, 3], \\\"param2\\\": {\\\"a\\\": true, \\\"b\\\": false} }"}
        {"formula": "Mathematical expressions: {\\\"equation\\\": \\\"x^2 + y^2 = r^2\\\"}"}
        """
    ).strip()

    ndjson_parser = NdJsonParser(s=test_str)
    parsed_rows = list(iter(ndjson_parser))

    assert isinstance(parsed_rows, list)
    assert len(parsed_rows) == 3

    assert parsed_rows[0]["content"] == 'Nested objects: {"key1": "value1", "key2": "value2"}'
    assert parsed_rows[1]["config"] == '{ "param1": [1, 2, 3], "param2": {"a": true, "b": false} }'
    assert parsed_rows[2]["formula"] == 'Mathematical expressions: {"equation": "x^2 + y^2 = r^2"}'
