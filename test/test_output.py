
import pytest
# noinspection PyProtectedMember
from pipethis._streamitem import LineStreamItem
# noinspection PyProtectedMember
from pipethis._transform import RegexKeepFilter, RegexSkipFilter, RegexSubstituteTransform
# noinspection PyProtectedMember
from pipethis._output import ToStdOut, ToFile,ToString


# noinspection SpellCheckingInspection
@pytest.mark.parametrize("lines", [
    # Test case with multiple lines
    [
        LineStreamItem(sequence_id=1, data="Line 1: Test", resource_name='test'),
        LineStreamItem(sequence_id=2, data="Line 2: More tests", resource_name='test'),
    ],
    # Test case with a single line
    [
        LineStreamItem(sequence_id=1, data="Single line test", resource_name='test'),
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
    with output as f:
        for lineinfo in lines:
            f.write(lineinfo)

    # Capture stdout
    captured = capsys.readouterr()

    # Build the expected output string from the `lines` list
    expected_output = "\n".join(lineinfo.data for lineinfo in lines) + ("\n" if lines else "")

    # Assert the captured stdout matches the expected output
    assert captured.out == expected_output


@pytest.mark.parametrize("lines", [
    # Test case with multiple lines
    [
        LineStreamItem(sequence_id=1, data="Line 1: File Test", resource_name='test'),
        LineStreamItem(sequence_id=2, data="Line 2: File Output", resource_name='test'),
    ],
    # Test case with a single line
    [
        LineStreamItem(sequence_id=1, data="Single line file test", resource_name='test'),
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
    with output as f:
        for lineinfo in lines:
            f.write(lineinfo)

    # Read the file back to validate
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Build the expected output string from the `lines` list
    expected_output = "\n".join(lineinfo.data for lineinfo in lines) + ("\n" if lines else "")

    # Assert the file content matches the expected output
    assert content == expected_output


@pytest.mark.parametrize(
    "pattern, lineinfo, is_yielded",
    [
        # Lines that DO NOT match the pattern should be yielded
        (r"skip.*", LineStreamItem(sequence_id=1, resource_name="ResourceA", data="do not skip this"), True),
        (r"skip.*", LineStreamItem(sequence_id=2, resource_name="ResourceB", data="skip this line"), False),
        (r"^\s*$", LineStreamItem(sequence_id=3, resource_name="ResourceC", data=" "), False),  # Empty or whitespace lines
        (r"^\s*$", LineStreamItem(sequence_id=4, resource_name="ResourceD", data="content"), True),
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
    "pattern, lineinfo, is_yielded",
    [
        # Lines that match the pattern should be yielded
        (r"keep.*", LineStreamItem(sequence_id=1, resource_name="ResourceA", data="keep this line"), True),
        (r"keep.*", LineStreamItem(sequence_id=2, resource_name="ResourceB", data="don't keep this"), False),
        (r"^\d+$", LineStreamItem(sequence_id=3, resource_name="ResourceC", data="12345"), True),  # Only digits
        (r"^\d+$", LineStreamItem(sequence_id=4, resource_name="ResourceD", data="abc123"), False),  # Contains non-digits
        (r".*error.*", LineStreamItem(sequence_id=5, resource_name="ResourceE", data="found an error here"), True),
        (r".*error.*", LineStreamItem(sequence_id=6, resource_name="ResourceF", data="successful operation"), False),
    ],
)
def test_regex_keep_filter(pattern, lineinfo, is_yielded):
    """
    Test the RegexKeepFilter to verify that only lines matching the pattern are kept.
    """
    transform = RegexKeepFilter(pattern)
    transformed = list(transform.transform(lineinfo))

    if is_yielded:
        # If the line should be yielded, ensure it's in the output
        assert len(transformed) == 1
        assert transformed[0] == lineinfo
    else:
        # If the line should be filtered out, ensure it's not in the output
        assert len(transformed) == 0

@pytest.mark.parametrize(
    "pattern, replacement, lineinfo, expected_line",
    [
        # Perform substitutions based on the regex pattern
        (r"foo", "bar", LineStreamItem(sequence_id=1, resource_name="ResourceA", data="foo is fun"), "bar is fun"),
        (r"\d+", "number", LineStreamItem(sequence_id=2, resource_name="ResourceB", data="123 is a number"),
         "number is a number"),
        (r"cat|dog", "animal", LineStreamItem(sequence_id=3, resource_name="ResourceC", data="dog chased the cat"),
         "animal chased the animal"),
        (r"skip this", "do this", LineStreamItem(sequence_id=4, resource_name="ResourceD", data="should I skip this?"),
         "should I do this?"),
        (r"no match", "nothing", LineStreamItem(sequence_id=5, resource_name="ResourceE", data="this line remains unchanged"),
         "this line remains unchanged"),
    ],
)
def test_regex_substitute_transform(pattern, replacement, lineinfo, expected_line):
    """
    Test the RegexSubstituteTransform to verify regex substitutions are applied correctly.
    """
    transform = RegexSubstituteTransform(pattern, replacement)
    transformed = list(transform.transform(lineinfo))

    assert len(transformed) == 1  # Only one LineStreamItem object should be yielded
    assert transformed[0].data == expected_line  # Ensure the substitution produced the expected line
    assert transformed[0].sequence_id == lineinfo.sequence_id  # Other attributes remain unchanged
    assert transformed[0].resource_name == lineinfo.resource_name


def test_string_output():
    # Arrange
    string_output = ToString()  # Create instance of ToString
    line1 = LineStreamItem(sequence_id=1, resource_name="stringA", data="First line")
    line2 = LineStreamItem(sequence_id=2, resource_name="stringA", data="Second line")
    line3 = LineStreamItem(sequence_id=3, resource_name="stringA", data="Third line")

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
    line1 = LineStreamItem(sequence_id=1, resource_name="stringA", data="")
    line2 = LineStreamItem(sequence_id=2, resource_name="stringA", data="Valid line")

    # Act
    string_output.write(line1)  # Writing an empty line
    string_output.write(line2)  # Writing a valid line after empty line

    # Assert
    expected_output = "\nValid line\n"
    assert string_output.text_output == expected_output

def test_to_string_context_manager():
    # Test data
    line1 = LineStreamItem(sequence_id=1, resource_name="1", data="First line")
    line2 = LineStreamItem(sequence_id=2, resource_name="2", data="Second line")

    # Use ToString in a context manager
    with ToString() as to_string:
        # Write the test data to the ToString instance
        to_string.write(line1)
        to_string.write(line2)

        # Verify the concatenated output matches the expected result
        expected_output = "First line\nSecond line\n"
        assert to_string.text_output == expected_output

# Assuming ToStdOut is implemented and overrides `write`
# noinspection SpellCheckingInspection
def test_to_stdout_context_manager(capsys):
    """
    Test that ToStdOut properly writes to standard output
    and supports context manager functionality.
    """

    line1 = LineStreamItem(sequence_id=1, resource_name="1", data="First line")
    line2 = LineStreamItem(sequence_id=2, resource_name="2", data="Second line")

    # Use ToStdOut inside a context manager
    with ToStdOut() as to_stdout:
        # Write to the ToStdOut object
        to_stdout.write(line1)
        to_stdout.write(line2)

    # Capture the output from stdout
    captured = capsys.readouterr()
    expected_output = "First line\nSecond line\n"

    # Assert the captured stdout matches the expected output
    assert captured.out == expected_output
