import pytest
from pipethis._input_from_string import FromString
from pipethis._input_from_strings import  FromStrings

def test_from_string_initialization():
    """Test initialization of FromString with valid inputs."""
    text = "Hello\nWorld\nPython"
    instance = FromString(name="example", text=text, sep="\n")
    assert instance.name == "example"
    assert instance.text == text
    assert instance.sep == "\n"


def test_from_string_stream():
    """Test the stream() method for streaming content."""
    text = "Line1\nLine2\nLine3"
    instance = FromString(name="test", text=text, sep="\n")
    expected_lines = ["Line1", "Line2", "Line3"]
    actual_lines = [item.data for item in instance.stream()]
    assert actual_lines == expected_lines  # Assuming stream() yields lines


def test_from_string_context_manager():
    """Test FromString as a context manager."""
    text = "A\nB\nC"
    with FromString(name="ctx", text=text, sep="\n") as instance:
        assert instance  # Ensure instance is not None
        actual_lines = [item.data for item in instance.stream()]
        assert actual_lines == ["A", "B", "C"]

def test_from_strings_initialization():
    """Test initialization of FromStrings with valid inputs."""
    lines = ["Hello", "World", "Python"]
    instance = FromStrings(name="example", lines=lines, sep="\n")
    assert instance.name == "example"
    assert instance.lines == lines
    assert instance.sep == "\n"

def test_from_strings_empty_lines():
    """Test that FromStrings initializes correctly with an empty lines input."""
    lines = []
    instance = FromStrings(name="empty", lines=lines, sep="\n")
    assert instance.name == "empty"
    assert instance.lines == lines
    assert instance.sep == "\n"

    # Validate streaming with no lines
    assert list(instance.stream()) == []

def test_from_strings_stream():
    """Test the stream() method for streaming content from FromStrings."""
    lines = ["Line1", "Line2", "Line3"]
    instance = FromStrings(name="test", lines=lines, sep="\n")
    expected_lines = ["Line1", "Line2", "Line3"]

    # Extract data from StreamItem instances in the stream
    actual_lines = [item.data for item in instance.stream()]

    assert actual_lines == expected_lines

def test_from_strings_with_context_manager():
    """Test FromStrings as a context manager."""
    lines = ["Item1", "Item2", "Item3"]
    with FromStrings(name="ctx_test", lines=lines, sep="\n") as instance:
        assert instance  # Ensure instance is not None
        actual_items = [item for item in instance.stream()]
        actual_lines = [item.data for item in actual_items]
        assert actual_lines == ["Item1", "Item2", "Item3"]
        actual_names = {item.resource_name for item in actual_items}
        assert len(actual_names) == 3
        assert actual_names == {"ctx_test-1","ctx_test-2","ctx_test-3"}

def test_from_strings_stream_with_trailing_empty_lines():
    """Test that trailing empty lines in FromStrings are handled correctly."""
    lines = ["First line", "Second line", ""]
    instance = FromStrings(name="with_empty", lines=lines, sep="\n")
    expected_lines = ["First line", "Second line", ""]

    # Extract data from StreamItem instances in the stream
    actual_lines = [item.data for item in instance.stream()]

    assert actual_lines == expected_lines



@pytest.mark.parametrize(
    "lines, sep, expected_data",
    [
        # Case 1: Input lines with no sep present
        (["A-B-C"], "-", ["A", "B", "C"]),

        # Case 2: Multiple lines with no separators in some
        (["Line1-Line2", "Line3"], "-", ["Line1", "Line2", "Line3"]),

        # Case 3: Lines with spaces used as separators
        (["Word1 Word2", "Word3"], " ", ["Word1", "Word2", "Word3"]),

        # Case 4: Separator present multiple times in one string
        (["Part1|Part2|Part3"], "|", ["Part1", "Part2", "Part3"]),

        # Case 5: Separator causing empty strings in the split
        (["One stop.", "End"], ".", ["One stop", "", "End"]),

        # Case 6: Leading and trailing separators
        (["|Start|Middle|End|"], "|", ["", "Start", "Middle", "End", ""]),

        # Case 7: A line with no sep present
        (["SingleLine"], ",", ["SingleLine"]),

        # Case 8: Completely empty input
        ([], "\n", []),

        # Case 9: Newlines with additional separators
        (["A-B", "C-D", "E"], "-", ["A", "B", "C", "D", "E"]),

        # Case 10: Handling empty lines
        (["", "Start|Middle|End", ""], "|", ["", "Start", "Middle", "End", ""]),
    ]
)
def test_from_strings_stream_splitting(lines, sep, expected_data):
    """Test FromStrings.stream with different sep use cases."""
    # Initialize the FromStrings class with name, lines, and sep
    instance = FromStrings(name="param_test", lines=lines, sep=sep)

    # Collect the actual data from the stream
    actual_data = [item.data for item in instance.stream()]

    # Assert the actual output matches the expected output
    assert actual_data == expected_data


