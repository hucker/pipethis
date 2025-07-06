import pytest

from _lineinfo import LineInfo
from _transform import SkipRepeatedBlankLines, UpperCase, LowerCase, AddMetaData,PassThrough

@pytest.mark.parametrize(
    "lineinfo, expected_line",
    [
        (LineInfo(sequence_id=1, resource_name="ResourceA", data="hello world"), "HELLO WORLD"),
        (LineInfo(sequence_id=2, resource_name="ResourceB", data="PyTest"), "PYTEST"),
        (LineInfo(sequence_id=3, resource_name="ResourceC", data=""), ""),  # Empty line remains unchanged
    ],
)
def test_uppercase_transform(lineinfo, expected_line):
    """
    Test the UpperCase transform to verify lines are converted to uppercase.
    """
    transform = UpperCase()
    transformed = list(transform.transform(lineinfo))  # Transform the line
    assert len(transformed) == 1  # Ensure only one LineInfo is yielded
    assert transformed[0].data == expected_line  # Ensure the line is uppercase

@pytest.mark.parametrize(
    "lineinfo, expected_line",
    [
        (LineInfo(sequence_id=1, resource_name="ResourceA", data="HELLO WORLD"), "hello world"),
        (LineInfo(sequence_id=2, resource_name="ResourceB", data="PyTEST"), "pytest"),
        (LineInfo(sequence_id=3, resource_name="ResourceC", data=""), ""),  # Empty line remains unchanged
    ],
)
def test_lowercase_transform(lineinfo, expected_line):
    """
    Test the LowerCase transform to verify lines are converted to lowercase.
    """
    transform = LowerCase()
    transformed = list(transform.transform(lineinfo))  # Transform the line
    assert len(transformed) == 1  # Ensure only one LineInfo is yielded
    assert transformed[0].data == expected_line  # Ensure the line is lowercase

@pytest.mark.parametrize(
    "lineinfo, expected_line",
    [
        (LineInfo(sequence_id=1, resource_name="ResourceA", data="hello world"), "ResourceA:1:hello world"),
        (LineInfo(sequence_id=2, resource_name="ResourceB", data="PyTest"), "ResourceB:2:PyTest"),
        (LineInfo(sequence_id=3, resource_name="ResourceC", data=""), "ResourceC:3:"),
    ],
)
def test_add_metadata_transform(lineinfo, expected_line):
    """
    Test the AddMetaData transform to verify metadata is correctly added to lines.
    """
    transform = AddMetaData()
    transformed = list(transform.transform(lineinfo))  # Transform the line
    assert len(transformed) == 1  # Ensure only one LineInfo is yielded
    assert transformed[0].data == expected_line  # Ensure metadata is correctly added

@pytest.mark.parametrize(
    "input_lines, expected_output_lines",
    [
        # Case 1: No blank lines
        (
                [
                    LineInfo(sequence_id=1, resource_name="ResourceA", data="First line"),
                    LineInfo(sequence_id=2, resource_name="ResourceA", data="Second line"),
                    LineInfo(sequence_id=3, resource_name="ResourceA", data="Third line"),
                ],
                [
                    LineInfo(sequence_id=1, resource_name="ResourceA", data="First line"),
                    LineInfo(sequence_id=2, resource_name="ResourceA", data="Second line"),
                    LineInfo(sequence_id=3, resource_name="ResourceA", data="Third line"),
                ],
        ),
        # Case 2: Blank lines at start
        (
                [
                    LineInfo(sequence_id=1, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=2, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=3, resource_name="ResourceA", data="First line"),
                    LineInfo(sequence_id=4, resource_name="ResourceA", data="Second line"),
                ],
                [
                    LineInfo(sequence_id=1, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=3, resource_name="ResourceA", data="First line"),
                    LineInfo(sequence_id=4, resource_name="ResourceA", data="Second line"),
                ],
        ),
        # Case 3: Blank lines in the middle
        (
                [
                    LineInfo(sequence_id=1, resource_name="ResourceA", data="First line"),
                    LineInfo(sequence_id=2, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=3, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=4, resource_name="ResourceA", data="Second line"),
                ],
                [
                    LineInfo(sequence_id=1, resource_name="ResourceA", data="First line"),
                    LineInfo(sequence_id=2, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=4, resource_name="ResourceA", data="Second line"),
                ],
        ),
        # Case 4: Blank lines at the end
        (
                [
                    LineInfo(sequence_id=1, resource_name="ResourceA", data="First line"),
                    LineInfo(sequence_id=2, resource_name="ResourceA", data="Second line"),
                    LineInfo(sequence_id=3, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=4, resource_name="ResourceA", data=""),
                ],
                [
                    LineInfo(sequence_id=1, resource_name="ResourceA", data="First line"),
                    LineInfo(sequence_id=2, resource_name="ResourceA", data="Second line"),
                    LineInfo(sequence_id=3, resource_name="ResourceA", data=""),
                ],
        ),
        # Case 5: Blank lines scattered across
        (
                [
                    LineInfo(sequence_id=1, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=2, resource_name="ResourceA", data="First line"),
                    LineInfo(sequence_id=3, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=4, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=5, resource_name="ResourceA", data="Second line"),
                    LineInfo(sequence_id=6, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=7, resource_name="ResourceA", data=""),
                ],
                [
                    LineInfo(sequence_id=1, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=2, resource_name="ResourceA", data="First line"),
                    LineInfo(sequence_id=3, resource_name="ResourceA", data=""),
                    LineInfo(sequence_id=5, resource_name="ResourceA", data="Second line"),
                    LineInfo(sequence_id=6, resource_name="ResourceA", data=""),
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
        assert transformed_line.data == expected_line.data
        assert transformed_line.sequence_id == expected_line.sequence_id
        assert transformed_line.resource_name == expected_line.resource_name

def test_pass_through_single():
    """Test the PassThrough transform with a single LineInfo input."""
    # Arrange
    transform = PassThrough()
    lineinfo = LineInfo(sequence_id=1, resource_name="test.txt", data="Hello, World!")

    # Act
    result = list(transform.transform(lineinfo))

    # Assert
    # It should return exactly the same object with no modifications
    assert len(result) == 1
    assert result[0] is lineinfo
    assert result[0].data == "Hello, World!"
    assert result[0].sequence_id == 1
    assert result[0].resource_name == "test.txt"

def test_pass_through_multiple():
    """Test the PassThrough transform with multiple LineInfo inputs."""
    # Arrange
    transform = PassThrough()
    lineinfo_list = [
        LineInfo(sequence_id=1, resource_name="file1.txt", data="Line 1"),
        LineInfo(sequence_id=2, resource_name="file1.txt", data="Line 2"),
        LineInfo(sequence_id=3, resource_name="file2.txt", data="Line A"),
    ]

    # Act
    result = [list(transform.transform(info))[0] for info in lineinfo_list]

    # Assert
    # Each output should be exactly the same as its corresponding input
    assert len(result) == len(lineinfo_list)
    for original, transformed in zip(lineinfo_list, result):
        assert transformed is original
        assert transformed.data == original.data
        assert transformed.sequence_id == original.sequence_id
        assert transformed.resource_name == original.resource_name
