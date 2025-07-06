import pytest
from _lineinfo import LineInfo


def test_lineinfo_initialization():
    """Test that LineInfo initializes correctly with provided values."""
    lineinfo = LineInfo(line_number=1, resource_name="test.txt", line="Hello, World!")

    assert lineinfo.line_number == 1
    assert lineinfo.resource_name == "test.txt"
    assert lineinfo.line == "Hello, World!"


def test_lineinfo_equality_same_values():
    """Test that two LineInfo objects with the same values are equal."""
    line1 = LineInfo(line_number=1, resource_name="test.txt", line="Hello, World!")
    line2 = LineInfo(line_number=1, resource_name="test.txt", line="Hello, World!")

    assert line1 == line2


def test_lineinfo_equality_different_values():
    """Test that two LineInfo objects with different values are not equal."""
    line1 = LineInfo(line_number=1, resource_name="test.txt", line="Hello, World!")
    line2 = LineInfo(line_number=2, resource_name="test.txt", line="Goodbye, World!")

    assert line1 != line2


def test_lineinfo_equality_with_different_object_type():
    """Test that LineInfo is not equal to objects of a different type."""
    lineinfo = LineInfo(line_number=1, resource_name="test.txt", line="Hello, World!")
    other_object = {"line_number": 1, "resource_name": "test.txt", "line": "Hello, World!"}

    assert lineinfo != other_object


def test_lineinfo_representation():
    """Test the string representation (__repr__) of LineInfo."""
    lineinfo = LineInfo(line_number=1, resource_name="test.txt", line="Hello, World!")
    expected_repr = "LineInfo(line_number=1, resource_name='test.txt', line='Hello, World!')"

    assert repr(lineinfo) == expected_repr


def test_lineinfo_line_number_validation():
    """Optional: Test that invalid line numbers raise an error."""
    with pytest.raises(ValueError):
        LineInfo(line_number=-1, resource_name="test.txt", line="Invalid line")


@pytest.mark.parametrize(
    "line_number, resource_name, line",
    [
        (1, "test.txt", "Content of the line"),  # Valid case
        (42, "data.csv", ""),  # Valid: empty line content is acceptable
        (100, "file.py", "Python code example"),  # Valid case
    ],
)
def test_valid_lineinfo(line_number, resource_name, line):
    """Test that valid LineInfo objects can be created."""
    lineinfo = LineInfo(line_number=line_number, resource_name=resource_name, line=line)
    assert lineinfo.line_number == line_number
    assert lineinfo.resource_name == resource_name
    assert lineinfo.line == line


@pytest.mark.parametrize(
    "line_number, expected_error",
    [
        (0, "line_number must be an integer greater than 0"),  # Zero is not valid
        (-10, "line_number must be an integer greater than 0"),  # Negative number
        ("1", "line_number must be an integer greater than 0"),  # Non-integer
        (None, "line_number must be an integer greater than 0"),  # NoneType
    ],
)
def test_invalid_line_number(line_number, expected_error):
    """Test that invalid line_number values raise ValueError."""
    with pytest.raises(ValueError, match=expected_error):
        LineInfo(line_number=line_number, resource_name="test.txt", line="Content")


@pytest.mark.parametrize(
    "resource_name, expected_error",
    [
        ("", "resource_name must be a non-empty string"),  # Empty string
        ("   ", "resource_name must be a non-empty string"),  # Whitespace-only string
        (None, "resource_name must be a non-empty string"),  # NoneType
        (123, "resource_name must be a non-empty string"),  # Non-string type
    ],
)
def test_invalid_resource_name(resource_name, expected_error):
    """Test that invalid resource_name values raise ValueError."""
    with pytest.raises(ValueError, match=expected_error):
        LineInfo(line_number=1, resource_name=resource_name, line="Content")


@pytest.mark.parametrize(
    "line, expected_error",
    [
        (None, "line must be a string"),  # NoneType
        (123, "line must be a string"),  # Non-string type
        ([], "line must be a string"),  # List instead of string
    ],
)
def test_invalid_line(line, expected_error):
    """Test that invalid line values raise ValueError."""
    with pytest.raises(ValueError, match=expected_error):
        LineInfo(line_number=1, resource_name="test.txt", line=line)

