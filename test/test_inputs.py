import pytest
import pathlib
from pipethis._inputs import FromString,FromFolder,FromFile,FromGlob,FromStrings
from pipethis._file_handler import TextFileHandler


@pytest.fixture(autouse=True)
def reset_handlers():
    """
    Fixture to reset registered handlers before each test.
    """
    FromFile.clear_registered_handlers()


def test_from_string_basic():
    """Test `FromString` with basic multiline input."""
    text = "Hello\nWorld\nThis is a test\nLine 4"
    from_string = FromString(text)
    results = list(from_string.stream())

    assert len(results) == 4

    assert results[0].sequence_id == 1
    assert results[0].resource_name == "text"
    assert results[0].data == "Hello"

    assert results[1].sequence_id == 2
    assert results[1].resource_name == "text"
    assert results[1].data == "World"

    assert results[2].sequence_id == 3
    assert results[2].resource_name == "text"
    assert results[2].data == "This is a test"

    assert results[3].sequence_id == 4
    assert results[3].resource_name == "text"
    assert results[3].data == "Line 4"

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

def test_from_strings_single_line():
    """Test `FromString` with a single-line input."""
    lines = ["This is a single-line test."]
    from_strings = FromStrings(lines,separator='\n',name="text")
    results = list(from_strings.stream())

    assert len(results) == 1
    assert results[0].sequence_id == 1


def test_from_strings_single_line():
    """Test `FromString` with a single-line input."""
    lines = ["This is the first line.","This is the next line"]
    from_strings = FromStrings(lines,separator='\n',name="text")
    results = list(from_strings.stream())

    assert len(results) == 2
    assert results[0].sequence_id == 1
    assert results[0].data == "This is the first line."
    assert results[1].sequence_id == 1
    assert results[1].data == "This is the next line"


def test_from_strings_single_line():
    """Test `FromString` with a single-line input."""
    lines = ["This is the\nfirst line.","This is the\nnext line"]
    from_strings = FromStrings(lines,separator='\n',name="text")
    results = list(from_strings.stream())

    assert len(results) == 2
    assert results[0].sequence_id == 1
    assert results[0].data == "This is the"
    assert results[1].sequence_id == 2
    assert results[1].data == "first line."

    assert results[2].sequence_id == 1
    assert results[2].data == "This is the"
    assert results[3].sequence_id == 2
    assert results[3].data == "next line"


@pytest.mark.parametrize(
    "lines, expected_results",
    [
        (
                # Test 1: Single-line input
                ["This is a single-line test."],
                [
                    (1, "This is a single-line test.")
                ],
        ),
        (
                # Test 2: Two single-line inputs
                ["This is the first line.", "This is the next line"],
                [
                    (1, "This is the first line."),
                    (1, "This is the next line"),
                ],
        ),
        (
                # Test 3: Multi-line inputs separated by '\n'
                ["This is the\nfirst line.", "This is the\nnext line"],
                [
                    (1, "This is the"),
                    (2, "first line."),
                    (1, "This is the"),
                    (2, "next line"),
                ],
        ),
        (
                # Test 4: Null string input
                [""],
                [(1,"")]
        ),

    ],
)
def test_from_strings(lines, expected_results):
    """Test `FromStrings` with various input cases including null/empty strings."""

    from_strings = FromStrings(lines, separator='\n', name="text")
    results = list(from_strings.stream())

    # Assert that the length of the results matches the expected results length
    assert len(results) == len(expected_results)

    # Verify sequence_id and data for each result if results are not empty
    for result, (expected_sequence_id, expected_data) in zip(results, expected_results):
        assert result.sequence_id == expected_sequence_id
        assert result.data == expected_data



@pytest.fixture(autouse=True)
def reset_handlers():
    """
    Fixture to reset registered handlers before each test.
    """
    FromFile.clear_registered_handlers()


def test_from_string_basic():
    """Test `FromString` with basic multiline input."""
    text = "Hello\nWorld\nThis is a test\nLine 4"
    from_string = FromString(text)
    results = list(from_string.stream())

    assert len(results) == 4

    assert results[0].sequence_id == 1
    assert results[0].resource_name == "text"
    assert results[0].data == "Hello"

    assert results[1].sequence_id == 2
    assert results[1].resource_name == "text"
    assert results[1].data == "World"

    assert results[2].sequence_id == 3
    assert results[2].resource_name == "text"
    assert results[2].data == "This is a test"

    assert results[3].sequence_id == 4
    assert results[3].resource_name == "text"
    assert results[3].data == "Line 4"

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

def test_from_strings_single_line():
    """Test `FromString` with a single-line input."""
    lines = ["This is a single-line test."]
    from_strings = FromStrings(lines,separator='\n',name="text")
    results = list(from_strings.stream())

    assert len(results) == 1
    assert results[0].sequence_id == 1
    assert results[0].data == "This is a single-line test."
    assert results[0].resource_name == "text-1"

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

# Pytest fixture to create temporary files for testing
@pytest.fixture
def sample_file(tmp_path):
    """Creates a sample temporary file for testing."""
    file_path = tmp_path / "test_file.txt"
    content = "Hello\nWorld\nThis is a test\nLine 4"
    file_path.write_text(content)
    return file_path


def test_from_file_basic(sample_file):
    """Test `FromFile` with a basic file."""
    with FromFile(sample_file, file_handler=TextFileHandler).file_handler as from_file:
        results = list(from_file.stream())

    assert len(results) == 4  # File has 4 lines

    assert results[0].sequence_id == 1
    assert results[0].resource_name == str(sample_file)
    assert results[0].data == "Hello"

    assert results[1].sequence_id == 2
    assert results[1].resource_name == str(sample_file)
    assert results[1].data == "World"

    assert results[2].sequence_id == 3
    assert results[2].resource_name == str(sample_file)
    assert results[2].data == "This is a test"

    assert results[3].sequence_id == 4
    assert results[3].resource_name == str(sample_file)
    assert results[3].data == "Line 4"


def test_from_file_empty_file(tmp_path):
    """Test `FromFile` with an empty file."""
    empty_file = tmp_path / "empty_file.txt"
    empty_file.write_text("")  # Create an empty file

    with FromFile(empty_file).file_handler as from_file:
        results = list(from_file.stream())

    assert len(results) == 0  # There should be no lines


def test_from_file_file_with_trailing_newlines(tmp_path):
    """Test `FromFile` with a file that has trailing newlines."""
    file_with_trailing_newlines = tmp_path / "trailing_newlines.txt"
    content = "Line 1\nLine 2\n\n"
    file_with_trailing_newlines.write_text(content)

    with FromFile(file_with_trailing_newlines).file_handler as from_file:
        results = list(from_file.stream())

    assert len(results) == 3  # Two lines with content, one blank line

    assert results[0].sequence_id == 1
    assert results[0].data == "Line 1"

    assert results[1].sequence_id == 2
    assert results[1].data == "Line 2"

    assert results[2].sequence_id == 3
    assert results[2].data == ""  # Blank line


def test_from_file_unicode_handling(tmp_path):
    """Test `FromFile` to handle files with Unicode characters."""
    unicode_file = tmp_path / "unicode_file.txt"
    content = "Héllo\nWörld\n你好\nこんにちは\n"
    unicode_file.write_text(content, encoding="utf-8")

    with FromFile(unicode_file).file_handler as from_file:
        results = list(from_file.stream())

    assert len(results) == 4

    assert results[0].sequence_id == 1
    assert results[0].data == "Héllo"

    assert results[1].sequence_id == 2
    assert results[1].data == "Wörld"

    assert results[2].sequence_id == 3
    assert results[2].data == "你好"

    assert results[3].sequence_id == 4
    assert results[3].data == "こんにちは"


# Pytest fixture to create temporary folder and files
@pytest.fixture
def folder_with_files(tmp_path):
    """Creates a temporary folder with test files for FromFolder testing."""
    folder = tmp_path / "test_folder"
    folder.mkdir()

    # Create files
    file1 = folder / "file1.txt"
    file1.write_text("This is file 1.\nLine 2 of file 1.")

    file2 = folder / "file2.log"
    file2.write_text("This is file 2.\nLine 2 of file 2.")

    file3 = folder / "file3.txt"
    file3.write_text("This is file 3.\nLine 2 of file 3.")

    file4 = folder / "file4.tmp"
    file4.write_text("This is file 4.\nLine 2 of file 4.")

    # Return the folder path
    return folder

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

# Pytest fixture to create temporary files for testing
@pytest.fixture
def sample_file(tmp_path):
    """Creates a sample temporary file for testing."""
    file_path = tmp_path / "test_file.txt"
    content = "Hello\nWorld\nThis is a test\nLine 4"
    file_path.write_text(content)
    return file_path


def test_from_file_basic(sample_file):
    """Test `FromFile` with a basic file."""
    with FromFile(sample_file, file_handler=TextFileHandler).file_handler as from_file:
        results = list(from_file.stream())

    assert len(results) == 4  # File has 4 lines

    assert results[0].sequence_id == 1
    assert results[0].resource_name == str(sample_file)
    assert results[0].data == "Hello"

    assert results[1].sequence_id == 2
    assert results[1].resource_name == str(sample_file)
    assert results[1].data == "World"

    assert results[2].sequence_id == 3
    assert results[2].resource_name == str(sample_file)
    assert results[2].data == "This is a test"

    assert results[3].sequence_id == 4
    assert results[3].resource_name == str(sample_file)
    assert results[3].data == "Line 4"


def test_from_file_empty_file(tmp_path):
    """Test `FromFile` with an empty file."""
    empty_file = tmp_path / "empty_file.txt"
    empty_file.write_text("")  # Create an empty file

    with FromFile(empty_file).file_handler as from_file:
        results = list(from_file.stream())

    assert len(results) == 0  # There should be no lines


def test_from_file_file_with_trailing_newlines(tmp_path):
    """Test `FromFile` with a file that has trailing newlines."""
    file_with_trailing_newlines = tmp_path / "trailing_newlines.txt"
    content = "Line 1\nLine 2\n\n"
    file_with_trailing_newlines.write_text(content)

    with FromFile(file_with_trailing_newlines).file_handler as from_file:
        results = list(from_file.stream())

    assert len(results) == 3  # Two lines with content, one blank line

    assert results[0].sequence_id == 1
    assert results[0].data == "Line 1"

    assert results[1].sequence_id == 2
    assert results[1].data == "Line 2"

    assert results[2].sequence_id == 3
    assert results[2].data == ""  # Blank line


def test_from_file_unicode_handling(tmp_path):
    """Test `FromFile` to handle files with Unicode characters."""
    unicode_file = tmp_path / "unicode_file.txt"
    content = "Héllo\nWörld\n你好\nこんにちは\n"
    unicode_file.write_text(content, encoding="utf-8")

    with FromFile(unicode_file).file_handler as from_file:
        results = list(from_file.stream())

    assert len(results) == 4

    assert results[0].sequence_id == 1
    assert results[0].data == "Héllo"

    assert results[1].sequence_id == 2
    assert results[1].data == "Wörld"

    assert results[2].sequence_id == 3
    assert results[2].data == "你好"

    assert results[3].sequence_id == 4
    assert results[3].data == "こんにちは"

