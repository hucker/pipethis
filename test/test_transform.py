import pytest

from _lineinfo import LineInfo
from _transform import SkipRepeatedBlankLines, UpperCase, LowerCase, AddMetaData

@pytest.mark.parametrize(
    "lineinfo, expected_line",
    [
        (LineInfo(line_number=1, resource_name="ResourceA", line="hello world"), "HELLO WORLD"),
        (LineInfo(line_number=2, resource_name="ResourceB", line="PyTest"), "PYTEST"),
        (LineInfo(line_number=3, resource_name="ResourceC", line=""), ""),  # Empty line remains unchanged
    ],
)
def test_uppercase_transform(lineinfo, expected_line):
    """
    Test the UpperCase transform to verify lines are converted to uppercase.
    """
    transform = UpperCase()
    transformed = list(transform.transform(lineinfo))  # Transform the line
    assert len(transformed) == 1  # Ensure only one LineInfo is yielded
    assert transformed[0].line == expected_line  # Ensure the line is uppercase

@pytest.mark.parametrize(
    "lineinfo, expected_line",
    [
        (LineInfo(line_number=1, resource_name="ResourceA", line="HELLO WORLD"), "hello world"),
        (LineInfo(line_number=2, resource_name="ResourceB", line="PyTEST"), "pytest"),
        (LineInfo(line_number=3, resource_name="ResourceC", line=""), ""),  # Empty line remains unchanged
    ],
)
def test_lowercase_transform(lineinfo, expected_line):
    """
    Test the LowerCase transform to verify lines are converted to lowercase.
    """
    transform = LowerCase()
    transformed = list(transform.transform(lineinfo))  # Transform the line
    assert len(transformed) == 1  # Ensure only one LineInfo is yielded
    assert transformed[0].line == expected_line  # Ensure the line is lowercase

@pytest.mark.parametrize(
    "lineinfo, expected_line",
    [
        (LineInfo(line_number=1, resource_name="ResourceA", line="hello world"), "ResourceA:1:hello world"),
        (LineInfo(line_number=2, resource_name="ResourceB", line="PyTest"), "ResourceB:2:PyTest"),
        (LineInfo(line_number=3, resource_name="ResourceC", line=""), "ResourceC:3:"),
    ],
)
def test_add_metadata_transform(lineinfo, expected_line):
    """
    Test the AddMetaData transform to verify metadata is correctly added to lines.
    """
    transform = AddMetaData()
    transformed = list(transform.transform(lineinfo))  # Transform the line
    assert len(transformed) == 1  # Ensure only one LineInfo is yielded
    assert transformed[0].line == expected_line  # Ensure metadata is correctly added

@pytest.mark.parametrize(
    "input_lines, expected_output_lines",
    [
        # Case 1: No blank lines
        (
                [
                    LineInfo(line_number=1, resource_name="ResourceA", line="First line"),
                    LineInfo(line_number=2, resource_name="ResourceA", line="Second line"),
                    LineInfo(line_number=3, resource_name="ResourceA", line="Third line"),
                ],
                [
                    LineInfo(line_number=1, resource_name="ResourceA", line="First line"),
                    LineInfo(line_number=2, resource_name="ResourceA", line="Second line"),
                    LineInfo(line_number=3, resource_name="ResourceA", line="Third line"),
                ],
        ),
        # Case 2: Blank lines at start
        (
                [
                    LineInfo(line_number=1, resource_name="ResourceA", line=""),
                    LineInfo(line_number=2, resource_name="ResourceA", line=""),
                    LineInfo(line_number=3, resource_name="ResourceA", line="First line"),
                    LineInfo(line_number=4, resource_name="ResourceA", line="Second line"),
                ],
                [
                    LineInfo(line_number=1, resource_name="ResourceA", line=""),
                    LineInfo(line_number=3, resource_name="ResourceA", line="First line"),
                    LineInfo(line_number=4, resource_name="ResourceA", line="Second line"),
                ],
        ),
        # Case 3: Blank lines in the middle
        (
                [
                    LineInfo(line_number=1, resource_name="ResourceA", line="First line"),
                    LineInfo(line_number=2, resource_name="ResourceA", line=""),
                    LineInfo(line_number=3, resource_name="ResourceA", line=""),
                    LineInfo(line_number=4, resource_name="ResourceA", line="Second line"),
                ],
                [
                    LineInfo(line_number=1, resource_name="ResourceA", line="First line"),
                    LineInfo(line_number=2, resource_name="ResourceA", line=""),
                    LineInfo(line_number=4, resource_name="ResourceA", line="Second line"),
                ],
        ),
        # Case 4: Blank lines at the end
        (
                [
                    LineInfo(line_number=1, resource_name="ResourceA", line="First line"),
                    LineInfo(line_number=2, resource_name="ResourceA", line="Second line"),
                    LineInfo(line_number=3, resource_name="ResourceA", line=""),
                    LineInfo(line_number=4, resource_name="ResourceA", line=""),
                ],
                [
                    LineInfo(line_number=1, resource_name="ResourceA", line="First line"),
                    LineInfo(line_number=2, resource_name="ResourceA", line="Second line"),
                    LineInfo(line_number=3, resource_name="ResourceA", line=""),
                ],
        ),
        # Case 5: Blank lines scattered across
        (
                [
                    LineInfo(line_number=1, resource_name="ResourceA", line=""),
                    LineInfo(line_number=2, resource_name="ResourceA", line="First line"),
                    LineInfo(line_number=3, resource_name="ResourceA", line=""),
                    LineInfo(line_number=4, resource_name="ResourceA", line=""),
                    LineInfo(line_number=5, resource_name="ResourceA", line="Second line"),
                    LineInfo(line_number=6, resource_name="ResourceA", line=""),
                    LineInfo(line_number=7, resource_name="ResourceA", line=""),
                ],
                [
                    LineInfo(line_number=1, resource_name="ResourceA", line=""),
                    LineInfo(line_number=2, resource_name="ResourceA", line="First line"),
                    LineInfo(line_number=3, resource_name="ResourceA", line=""),
                    LineInfo(line_number=5, resource_name="ResourceA", line="Second line"),
                    LineInfo(line_number=6, resource_name="ResourceA", line=""),
                ],
        ),
    ],
)
def test_skip_repeated_blank_lines(input_lines, expected_output_lines):
    """
    Test SkipRepeatedBlankLines to verify that repeated blank lines are skipped
    while maintaining single blank lines and non-blank lines.
    """
    transform = SkipRepeatedBlankLines()
    transformed = list(line for lineinfo in input_lines for line in transform.transform(lineinfo))

    # Ensure the number of lines matches expected output
    assert len(transformed) == len(expected_output_lines)

    # Ensure that all lines are exactly as expected
    for transformed_line, expected_line in zip(transformed, expected_output_lines):
        assert transformed_line.line == expected_line.line
        assert transformed_line.line_number == expected_line.line_number
        assert transformed_line.resource_name == expected_line.resource_name
