import pathlib
import pytest
# noinspection PyProtectedMember
from pipethis._inputs import FromFolder
# noinspection PyProtectedMember
from pipethis._file_handler import TextFileHandler


# Pytest fixture to create temporary folder and files
@pytest.fixture
def folder_with_files(tmp_path):
    """Creates a temporary folder with test files for FromFolder testing."""
    folder = tmp_path / "test_folder"
    folder.mkdir()

    # This verifies that we don't dive into folders
    empty_folder = folder / "empty_folder"
    empty_folder.mkdir()

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

    with FromFolder(folder_path=folder_with_files,
                    file_handler=TextFileHandler,
                    keep_patterns=["*.txt", "*.log", "*.tmp"]) as from_folder:
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



# List of test cases for valid scenarios
@pytest.mark.parametrize(
    "keep_patterns, ignore_patterns, expected_files",
    [
        # Case 1: Include all files using keep_patterns
        (["*.txt", "*.log", "*.tmp"], None, {"file1.txt", "file2.log", "file3.txt", "file4.tmp"}),

        # Case 2: Only text files
        (["*.txt"], None, {"file1.txt", "file3.txt"}),

        # Case 3: Only log files
        (["*.log"], None, {"file2.log"}),

        # Case 4: Include all files except *.tmp
        (None, ["*.tmp"], {"file1.txt", "file2.log", "file3.txt"}),

        # Case 5: Only tmp files
        (["*.tmp"], None, {"file4.tmp"}),

        # Case 6: Exclude all files
        (None, ["*"], set()),

        # Case 7: No filters applied - all files included
        (None, None, {"file1.txt", "file2.log", "file3.txt", "file4.tmp"}),
    ]
)
def test_folder_filtering(folder_with_files, keep_patterns, ignore_patterns, expected_files):
    """Test valid filtering logic using keep_patterns or ignore_patterns."""
    with FromFolder(
            folder_path=folder_with_files,
            file_handler=TextFileHandler,
            keep_patterns=keep_patterns,
            ignore_patterns=ignore_patterns
    ) as from_folder:
        # Get files processed through the filtering logic
        results = list(from_folder.stream())

    # Extract the filenames processed from results
    processed_files = {pathlib.Path(line_info.resource_name).name for line_info in results}

    # Assert the filtered files match the expected result
    assert processed_files == expected_files


def test_from_folder_empty_folder(tmp_path):
    """Test FromFolder with an empty folder."""
    empty_folder = tmp_path / "empty_folder"
    empty_folder.mkdir()

    from_folder = FromFolder(empty_folder)

    # No files to process
    results = list(from_folder.stream())

    assert len(results) == 0


def test_from_folder_conflicting_filters(folder_with_files):
    """Test FromFolder raises ValueError when both keep_patterns and ignore_patterns are provided."""
    with pytest.raises(ValueError, match="You can specify either keep_patterns or ignore_patterns, but not both."):
        FromFolder(folder_with_files, keep_patterns=[".txt"], ignore_patterns=[".log"])

