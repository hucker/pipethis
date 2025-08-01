import pathlib
import pytest
# noinspection PyProtectedMember
from pipethis._streamitem import LineStreamItem


def test_stream_item_initialization():
    """Test that LineStreamItem initializes correctly with provided values."""
    lineinfo = LineStreamItem(sequence_id=1, resource_name="test.txt", data="Hello, World!")

    assert lineinfo.sequence_id == 1
    assert lineinfo.resource_name == "test.txt"
    assert lineinfo.data == "Hello, World!"


def test_line_info_equality_same_values():
    """Test that two LineStreamItem objects with the same values are equal."""
    line1 = LineStreamItem(sequence_id=1, resource_name="test.txt", data="Hello, World!")
    line2 = LineStreamItem(sequence_id=1, resource_name="test.txt", data="Hello, World!")

    assert line1 == line2


def test_lineinfo_equality_different_values():
    """Test that two LineStreamItem objects with different values are not equal."""
    line1 = LineStreamItem(sequence_id=1, resource_name="test.txt", data="Hello, World!")
    line2 = LineStreamItem(sequence_id=2, resource_name="test.txt", data="Goodbye, World!")

    assert line1 != line2


def test_lineinfo_equality_with_different_object_type():
    """Test that LineStreamItem is not equal to objects of a different type."""
    lineinfo = LineStreamItem(sequence_id=1, resource_name="test.txt", data="Hello, World!")
    other_object = {"sequence_id": 1, "resource_name": "test.txt", "line": "Hello, World!"}

    assert lineinfo != other_object


def test_lineinfo_representation():
    """Test the string representation (__repr__) of LineStreamItem."""
    lineinfo = LineStreamItem(sequence_id=1, resource_name="test.txt", data="Hello, World!")
    expected_repr = "LineStreamItem(sequence_id=1, resource_name='test.txt', data='Hello, World!')"

    assert repr(lineinfo) == expected_repr


def test_lineinfo_line_number_validation():
    """Optional: Test that invalid line numbers raise an error."""
    with pytest.raises(ValueError):
        LineStreamItem(sequence_id=-1, resource_name="test.txt", data="Invalid line")


@pytest.mark.parametrize(
    "sequence_id, resource_name, line",
    [
        (1, "test.txt", "Content of the line"),  # Valid case
        (42, "data.csv", ""),  # Valid: empty line content is acceptable
        (100, "file.py", "Python code example"),  # Valid case
    ],
)
def test_valid_lineinfo(sequence_id, resource_name, line):
    """Test that valid LineStreamItem objects can be created."""
    lineinfo = LineStreamItem(sequence_id=sequence_id, resource_name=resource_name, data=line)
    assert lineinfo.sequence_id == sequence_id
    assert lineinfo.resource_name == resource_name
    assert lineinfo.data == line

@pytest.mark.parametrize(
    "sequence_id, resource_name, line, expected_resource_name",
    [
        (1, pathlib.Path("/example/path/to/test.txt"), "Content of the line", "test.txt"),  # Path object
        (42, pathlib.Path("data.csv"), "", "data.csv"),  # Relative path
        (100, pathlib.Path("dir/subdir/file.py"), "Python code example", "file.py"),  # Nested relative path
    ],
)
def test_valid_lineinfo_with_path(sequence_id, resource_name, line, expected_resource_name):
    """Test that valid LineStreamItem objects can be created with Path objects as resource_name."""
    lineinfo = LineStreamItem(sequence_id=sequence_id, resource_name=resource_name, data=line)
    assert lineinfo.sequence_id == sequence_id
    assert lineinfo.resource_name == expected_resource_name  # Ensure only file name and extension
    assert lineinfo.data == line


@pytest.mark.parametrize(
    "sequence_id",
    [
        0,  # Zero is not valid
        -10,  # Negative number
        "1",  # Non-integer
        None,  # NoneType
    ],
)
def test_invalid_line_number(sequence_id):
    """Test that invalid sequence_id values raise ValueError."""
    expected_error = "Sequence_id must be a positive integer"  # Shared error message
    with pytest.raises(ValueError, match=expected_error):
        LineStreamItem(sequence_id=sequence_id, resource_name="test.txt", data="Content")



@pytest.mark.parametrize(
    "resource_name",
    [
        "",  # Empty string
        "   ",  # Whitespace-only string
        None,  # NoneType
        123,  # Non-string type
    ],
)
def test_invalid_resource_name(resource_name):
    """Test that invalid resource_name values raise ValueError."""
    with pytest.raises(ValueError, match= "Resource_name must be a non-empty string"):
        LineStreamItem(sequence_id=1, resource_name=resource_name, data="Content")



@pytest.mark.parametrize(
    "line",
    [
        None,  # NoneType
        123,  # Non-string type
        [],  # List instead of string
    ],
)
def test_invalid_line(line):
    """Test that invalid line values raise ValueError."""
    expected_error = "Data must be a string for LineStreamItem"  # Error message shared across cases
    with pytest.raises(ValueError, match=expected_error):
        LineStreamItem(sequence_id=1, resource_name="test.txt", data=line)


