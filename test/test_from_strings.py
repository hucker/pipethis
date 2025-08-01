import pytest
from pipethis._inputs import FromString,FromStrings

def test_from_string_empty_input():
    """Test `FromString` with an empty string."""
    text = ""
    from_string = FromString(text)
    results = list(from_string.stream())

    assert len(results) == 1
    assert results[0].sequence_id == 1
    assert results[0].data == ""
    assert results[0].resource_name == "text"


def test_from_string_custom_separator():
    """Test `FromString` with a custom separator."""
    text = "Item1||Item2||Item3"
    from_string = FromString(text, separator="||")
    results = list(from_string.stream())

    assert len(results) == 3

    assert results[0].sequence_id == 1
    assert results[0].data == "Item1"
    assert results[1].sequence_id == 2
    assert results[1].data == "Item2"
    assert results[2].sequence_id == 3
    assert results[2].data == "Item3"

def test_from_string_single_line():
    """Test `FromString` with a single-line input."""
    text = "This is a single-line test."
    from_string = FromString(text)
    results = list(from_string.stream())

    assert len(results) == 1
    assert results[0].sequence_id == 1
    assert results[0].data == "This is a single-line test."
    assert results[0].resource_name == "text"

def test_from_string_trailing_and_leading_separators():
    """Test `FromString` with leading and trailing separators."""
    text = "\nLine1\nLine2\nLine3\n"
    from_string = FromString(text)
    results = list(from_string.stream())

    assert len(results) == 5
    assert results[0].sequence_id == 1
    assert results[0].data == ""  # Leading separator
    assert results[1].sequence_id == 2
    assert results[1].data == "Line1"
    assert results[2].sequence_id == 3
    assert results[2].data == "Line2"
    assert results[3].sequence_id == 4
    assert results[3].data == "Line3"
    assert results[4].sequence_id == 5
    assert results[4].data == ""  # Trailing separator


def test_from_strings_single_line_multi():
    """Test `FromString` with a single-line input."""
    lines = ["This is a single-line test.","foo"]
    from_strings = FromStrings(lines,separator='\n',name="text")
    results = list(from_strings.stream())

    assert len(results) == 2
    assert results[0].sequence_id == 1
    assert results[0].data == "This is a single-line test."
    assert results[0].resource_name == "text-1"

    assert results[1].sequence_id == 1
    assert results[1].data == "foo"
    assert results[1].resource_name == "text-2"

def test_from_strings_single_string_input():
    """Test that a single string passed to FromStrings is wrapped in a list."""
    single_line = "This is a single string"

    # Instantiate with a single string
    from_strings_instance = FromStrings(lines=single_line)

    # Assert the lines are correctly wrapped into a list
    assert from_strings_instance.lines == [single_line]