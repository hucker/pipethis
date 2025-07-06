

import pytest
import os

from _lineinfo import LineInfo
from _output import ToStdOut, ToFile,ToString


@pytest.mark.parametrize("lines", [
    # Test case with multiple lines
    [
        LineInfo(line_number=1,line="Line 1: Test",resource_name='test'),
        LineInfo(line_number=2, line="Line 2: More tests",resource_name='test'),
    ],
    # Test case with a single line
    [
        LineInfo(line_number=1, line="Single line test",resource_name='test'),
    ],
    # Test case with no lines (empty input)
    []
])
def test_standard_output(capsys, lines):
    """
    Test that ToStdOut correctly writes to stdout.
    """
    output = ToStdOut()

    # Write each line to the output
    for lineinfo in lines:
        output.write(lineinfo)

    # Capture stdout
    captured = capsys.readouterr()

    # Build the expected output string from the `lines` list
    expected_output = "\n".join(lineinfo.line for lineinfo in lines) + ("\n" if lines else "")

    # Assert the captured stdout matches the expected output
    assert captured.out == expected_output


@pytest.mark.parametrize("lines", [
    # Test case with multiple lines
    [
        LineInfo(line_number=1, line="Line 1: File Test",resource_name='test'),
        LineInfo(line_number=2, line="Line 2: File Output",resource_name='test'),
    ],
    # Test case with a single line
    [
        LineInfo(line_number=1, line="Single line file test",resource_name='test'),
    ],
    # Test case with no lines (empty input)
    []
])
def test_file_output(tmp_path, lines):
    """
    Test that ToFile correctly writes to a file.
    """
    # Create a temporary file path
    file_path = tmp_path / "output.txt"

    # Create the ToFile instance
    output = ToFile(file_name=str(file_path))

    # Write each line to the file
    for lineinfo in lines:
        output.write(lineinfo)

    # Close the file
    output.close()

    # Read the file back to validate
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Build the expected output string from the `lines` list
    expected_output = "\n".join(lineinfo.line for lineinfo in lines) + ("\n" if lines else "")

    # Assert the file content matches the expected output
    assert content == expected_output


import pytest
from _lineinfo import LineInfo
from _transform import RegexSkipFilter, RegexSubstituteTransform


@pytest.mark.parametrize(
    "pattern, lineinfo, is_yielded",
    [
        # Lines that DO NOT match the pattern should be yielded
        (r"skip.*", LineInfo(line_number=1, resource_name="ResourceA", line="do not skip this"), True),
        (r"skip.*", LineInfo(line_number=2, resource_name="ResourceB", line="skip this line"), False),
        (r"^\s*$", LineInfo(line_number=3, resource_name="ResourceC", line=" "), False),  # Empty or whitespace lines
        (r"^\s*$", LineInfo(line_number=4, resource_name="ResourceD", line="content"), True),
    ],
)
def test_regex_skip_filter(pattern, lineinfo, is_yielded):
    """
    Test the RegexSkipFilter to verify that lines matching the pattern are skipped.
    """
    transform = RegexSkipFilter(pattern)
    transformed = list(transform.transform(lineinfo))

    if is_yielded:
        # If the line should be yielded, ensure it's in the output
        assert len(transformed) == 1
        assert transformed[0] == lineinfo
    else:
        # If the line should be skipped, ensure it's not in the output
        assert len(transformed) == 0


@pytest.mark.parametrize(
    "pattern, replacement, lineinfo, expected_line",
    [
        # Perform substitutions based on the regex pattern
        (r"foo", "bar", LineInfo(line_number=1, resource_name="ResourceA", line="foo is fun"), "bar is fun"),
        (r"\d+", "number", LineInfo(line_number=2, resource_name="ResourceB", line="123 is a number"),
         "number is a number"),
        (r"cat|dog", "animal", LineInfo(line_number=3, resource_name="ResourceC", line="dog chased the cat"),
         "animal chased the animal"),
        (r"skip this", "do this", LineInfo(line_number=4, resource_name="ResourceD", line="should I skip this?"),
         "should I do this?"),
        (r"no match", "nothing", LineInfo(line_number=5, resource_name="ResourceE", line="this line remains unchanged"),
         "this line remains unchanged"),
    ],
)
def test_regex_substitute_transform(pattern, replacement, lineinfo, expected_line):
    """
    Test the RegexSubstituteTransform to verify regex substitutions are applied correctly.
    """
    transform = RegexSubstituteTransform(pattern, replacement)
    transformed = list(transform.transform(lineinfo))

    assert len(transformed) == 1  # Only one LineInfo object should be yielded
    assert transformed[0].line == expected_line  # Ensure the substitution produced the expected line
    assert transformed[0].line_number == lineinfo.line_number  # Other attributes remain unchanged
    assert transformed[0].resource_name == lineinfo.resource_name


def test_string_output():
    # Arrange
    string_output = ToString()  # Create instance of ToString
    line1 = LineInfo(line_number=1, resource_name="stringA", line="First line")
    line2 = LineInfo(line_number=2, resource_name="stringA", line="Second line")
    line3 = LineInfo(line_number=3, resource_name="stringA", line="Third line")

    # Act
    string_output.write(line1)
    string_output.write(line2)
    string_output.write(line3)

    # Assert
    expected_output = "First line\nSecond line\nThird line\n"
    assert string_output.text_output == expected_output


def test_string_output_empty_lineinfo():
    # Arrange
    string_output = ToString()
    line1 = LineInfo(line_number=1, resource_name="stringA", line="")
    line2 = LineInfo(line_number=2, resource_name="stringA", line="Valid line")

    # Act
    string_output.write(line1)  # Writing an empty line
    string_output.write(line2)  # Writing a valid line after empty line

    # Assert
    expected_output = "\nValid line\n"
    assert string_output.text_output == expected_output
