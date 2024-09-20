import pytest
from odp.client.dto.file_dto import FileMetadataDto


@pytest.mark.parametrize(
    "file_name, correct",
    [("test.txt", True), ("foo/bar/test2.txt", True), ("/test.txt", False), ("/foo/bar/test2.txt", False)],
)
def test_file_dto_names(file_name, correct):
    if correct:
        file_metadata = FileMetadataDto(name=file_name)
        assert file_metadata.name == file_name
    else:
        with pytest.raises(ValueError):
            FileMetadataDto(name=file_name)
