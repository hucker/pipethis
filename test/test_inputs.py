import pytest
import pathlib
from _inputs import FromString,FromFolder,FromFile,FromRGlob

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
    from_file = FromFile(sample_file)
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

    from_file = FromFile(empty_file)
    results = list(from_file.stream())

    assert len(results) == 0  # There should be no lines


def test_from_file_file_with_trailing_newlines(tmp_path):
    """Test `FromFile` with a file that has trailing newlines."""
    file_with_trailing_newlines = tmp_path / "trailing_newlines.txt"
    content = "Line 1\nLine 2\n\n"
    file_with_trailing_newlines.write_text(content)

    from_file = FromFile(file_with_trailing_newlines)
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

    from_file = FromFile(unicode_file)
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

def test_from_folder_all_files(folder_with_files):
    """Test FromFolder streaming all files without relying on order."""
    from_folder = FromFolder(folder_with_files)

    # Gather all lines streamed from all files
    results = list(from_folder.stream())

    # Ensure the total lines match expected (8 lines from 4 files)
    assert len(results) == 8

    # Create a dictionary to map filenames to expected lines
    expected_lines = {
        "file1.txt": ["This is file 1.", "Line 2 of file 1."],
        "file2.log": ["This is file 2.", "Line 2 of file 2."],
        "file3.txt": ["This is file 3.", "Line 2 of file 3."],
        "file4.tmp": ["This is file 4.", "Line 2 of file 4."],
    }

    # Check that all files and their lines are present (ignoring file iteration order)
    for line_info in results:
        filename = pathlib.Path(line_info.resource_name).name
        assert filename in expected_lines
        # Verify line content matches one of the expected lines for this file
        assert line_info.data in expected_lines[filename]



def test_from_folder_keep_extensions(folder_with_files):
    """Test FromFolder with keep_extensions filtering."""
    from_folder = FromFolder(folder_with_files, keep_extensions=[".txt"])

    # Only .txt files should be processed
    results = list(from_folder.stream())

    assert len(results) == 4  # 2 .txt files with 2 lines each

    # Expected data for .txt files
    expected_lines = {
        "file1.txt": ["This is file 1.", "Line 2 of file 1."],
        "file3.txt": ["This is file 3.", "Line 2 of file 3."],
    }

    # Create a tracker for filenames and their lines found
    processed_files = {}

    for line_info in results:
        filename = pathlib.Path(line_info.resource_name).name
        if filename not in processed_files:
            processed_files[filename] = []
        processed_files[filename].append(line_info.data)

    # Validate that all expected files and lines were processed
    assert processed_files == expected_lines

def test_from_folder_ignore_extensions(folder_with_files):
    """Test FromFolder with ignore_extensions filtering."""
    from_folder = FromFolder(folder_with_files, ignore_extensions=[".log", ".tmp"])

    # .log and .tmp files should be excluded
    results = list(from_folder.stream())

    assert len(results) == 4  # 2 .txt files with 2 lines each

    # Expected data for .txt files, grouped by their resource_name (file name)
    expected_lines = {
        "file1.txt": ["This is file 1.", "Line 2 of file 1."],
        "file3.txt": ["This is file 3.", "Line 2 of file 3."],
    }

    # Create a tracker for resource_name (filenames) and their processed lines
    processed_files = {}

    for line_info in results:
        # Use resource_name instead of filename
        resource_name = pathlib.Path(line_info.resource_name).name
        if resource_name not in processed_files:
            processed_files[resource_name] = []
        processed_files[resource_name].append(line_info.data)

    # Validate that all expected files and lines were processed (ignoring order)
    assert processed_files == expected_lines



def test_from_folder_empty_folder(tmp_path):
    """Test FromFolder with an empty folder."""
    empty_folder = tmp_path / "empty_folder"
    empty_folder.mkdir()

    from_folder = FromFolder(empty_folder)

    # No files to process
    results = list(from_folder.stream())

    assert len(results) == 0


def test_from_folder_conflicting_filters(folder_with_files):
    """Test FromFolder raises ValueError when both keep_extensions and ignore_extensions are provided."""
    with pytest.raises(ValueError, match="You can specify either keep_extensions or ignore_extensions, but not both."):
        FromFolder(folder_with_files, keep_extensions=[".txt"], ignore_extensions=[".log"])

def create_file(file_path: pathlib.Path, content: str = ""):
    """Helper function to create a file with specific content."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)

from tempfile import TemporaryDirectory
@pytest.fixture
def setup_files() -> pathlib.Path:
    """Fixture to set up a temporary folder structure for testing."""
    with TemporaryDirectory() as temp_dir:
        root = pathlib.Path(temp_dir)

        # Create files and folders
        create_file(root / "file1.txt", "File 1, Line 1\nFile 1, Line 2")
        create_file(root / "file2.log", "File 2, Line 1")
        create_file(root / "folder1" / "file3.txt", "File 3, Line 1\nFile 3, Line 2")
        create_file(root / "folder1" / "file4.tmp", "File 4, Line 1")
        create_file(root / "ignored_folder" / "file5.txt", "File 5, Line 1")
        create_file(root / "ignored_folder" / "file6.log", "File 6, Line 1")

        yield root


def test_from_rglob_all_files(setup_files):
    """Test FromRGlob streams all files without filtering."""
    from_rglob = FromRGlob(folder_path=setup_files)
    results = list(from_rglob.stream())

    # Check that all file lines are included
    expected_files = ["file1.txt", "file2.log", "file3.txt", "file4.tmp", "file5.txt", "file6.log"]
    assert len(results) == 8  # Total lines across all files
    assert all(pathlib.Path(line_info.resource_name).name in expected_files for line_info in results)

def test_from_rglob_keep_extensions(setup_files):
    """Test FromRGlob only keeps files with specified extensions."""
    from_rglob = FromRGlob(folder_path=setup_files, keep_extensions=[".txt"])
    results = list(from_rglob.stream())

    # Check that only .txt file lines are included
    expected_files = ["file1.txt", "file3.txt", "file5.txt"]
    assert len(results) == 5  # 2 lines from each .txt file
    assert all(pathlib.Path(line_info.resource_name).name in expected_files for line_info in results)

def test_from_rglob_ignore_extensions(setup_files):
    """Test FromRGlob ignores files with specified extensions."""
    from_rglob = FromRGlob(folder_path=setup_files, ignore_extensions=[".log", ".tmp"])
    results = list(from_rglob.stream())

    # Check that .log and .tmp files are excluded
    expected_files = ["file1.txt", "file3.txt", "file5.txt"]
    assert len(results) == 5  # 2 lines from each non-ignored file
    assert all(pathlib.Path(line_info.resource_name).name in expected_files for line_info in results)

def test_from_rglob_ignore_folders(setup_files):
    """Test FromRGlob ignores files in specified folders."""
    from_rglob = FromRGlob(folder_path=setup_files, ignore_folders=["ignored_folder"])
    results = list(from_rglob.stream())

    # Check that files from "ignored_folder" are excluded
    expected_files = ["file1.txt", "file2.log", "file3.txt", "file4.tmp"]
    assert len(results) == 6  # Total lines from non-ignored folders
    assert all(pathlib.Path(line_info.resource_name).name in expected_files for line_info in results)

def test_from_rglob_keep_extensions_and_ignore_folders(setup_files):
    """Test FromRGlob with both keep_extensions and ignore_folders."""
    from_rglob = FromRGlob(folder_path=setup_files, keep_extensions=[".txt"], ignore_folders=["ignored_folder"])
    results = list(from_rglob.stream())

    # Check that .txt files in "ignored_folder" are excluded
    expected_files = ["file1.txt", "file3.txt"]
    assert len(results) == 4  # 2 lines from each included file
    assert all(pathlib.Path(line_info.resource_name).name in expected_files for line_info in results)

def test_from_rglob_empty_folder():
    """Test FromRGlob with an empty folder."""
    with TemporaryDirectory() as temp_dir:
        folder = pathlib.Path(temp_dir)
        from_rglob = FromRGlob(folder_path=folder)
        results = list(from_rglob.stream())

        # Check that no files are processed
        assert len(results) == 0


def test_from_rglob_conflicting_arguments(setup_files):
    """Test FromRGlob raises an error with conflicting keep and ignore extensions."""
    with pytest.raises(ValueError, match="You can specify either keep_extensions or ignore_extensions, but not both."):
        FromRGlob(folder_path=setup_files, keep_extensions=[".txt"], ignore_extensions=[".log"])
