import pathlib
from tempfile import TemporaryDirectory
import pytest

from pipethis import TextFileHandler
# noinspection PyProtectedMember
from pipethis._inputs import FromGlob


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


def test_from_glob_all_files(setup_files):
    """Test FromGlob streams all files without filtering."""
    with FromGlob(folder_path=setup_files, file_handler=TextFileHandler) as from_rglob:
        results = list(from_rglob.stream())

    # Check that all file lines are included
    expected_files = {"file1.txt", "file2.log", "file3.txt", "file4.tmp", "file5.txt", "file6.log"}
    actual_files = {pathlib.Path(result.resource_name).name for result in results}
    assert expected_files == actual_files
    assert len(results) == 8

def test_from_glob_keep_extensions(setup_files):
    """Test FromGlob only keeps files with specified extensions."""
    with FromGlob(folder_path=setup_files, keep_patterns=["*.txt"]) as from_rglob:
        results = list(from_rglob.stream())

    # Check that only .txt file lines are included
    expected_files = {"file1.txt", "file3.txt", "file5.txt"}
    actual_files = {pathlib.Path(result.resource_name).name for result in results}
    assert len(results) == 5  # 2 lines from each .txt file
    assert expected_files == actual_files

def test_from_glob_ignore_extensions(setup_files):
    """Test FromGlob ignores files with specified extensions."""
    with FromGlob(folder_path=setup_files, ignore_patterns=["*.log", "*.tmp"]) as from_rglob:
        results = list(from_rglob.stream())

    # Check that .log and .tmp files are excluded
    expected_files = {"file1.txt", "file3.txt", "file5.txt"}
    actual_files = {pathlib.Path(result.resource_name).name for result in results}
    assert len(results) == 5  # 2 lines from each non-ignored file
    assert expected_files == actual_files


def test_from_glob_empty_folder():
    """Test FromGlob with an empty folder."""
    with TemporaryDirectory() as temp_dir:
        folder = pathlib.Path(temp_dir)
        from_rglob = FromGlob(folder_path=folder)
        results = list(from_rglob.stream())

        # Check that no files are processed
        assert len(results) == 0


def test_from_glob_conflicting_arguments(setup_files):
    """Test FromGlob raises an error with conflicting keep and ignore extensions."""
    with pytest.raises(ValueError, match="You can specify either keep_patterns or ignore_patterns, but not both."):
        FromGlob(folder_path=setup_files, keep_patterns=[".txt"], ignore_patterns=[".log"])

def test_from_glob_ignore_folders(setup_files):
    """Test FromGlob ignores files in specified folders."""
    from_rglob = FromGlob(folder_path=setup_files, ignore_folders=["ignored_folder"])
    results = list(from_rglob.stream())

    # Check that files from "ignored_folder" are excluded
    expected_files = ["file1.txt", "file2.log", "file3.txt", "file4.tmp"]
    assert len(results) == 6  # Total lines from non-ignored folders
    assert all(pathlib.Path(line_info.resource_name).name in expected_files for line_info in results)

def test_from_glob_keep_extensions_and_ignore_folders(setup_files):
    """Test FromGlob with both keep_patterns and ignore_folders."""
    from_rglob = FromGlob(folder_path=setup_files, keep_patterns=["*.txt"], ignore_folders=["ignored_folder"])
    results = list(from_rglob.stream())

    # Check that .txt files in "ignored_folder" are excluded
    expected_files = ["file1.txt", "file3.txt"]
    assert len(results) == 4  # 2 lines from each included file
    assert all(pathlib.Path(line_info.resource_name).name in expected_files for line_info in results)

def test_from_glob_invalid_folder_path():
    """Test that a ValueError is raised when the folder path is invalid."""
    with pytest.raises(ValueError, match="You can specify either keep_patterns or ignore_patterns, but not both."):
        FromGlob(folder_path="", keep_patterns=["*.txt"], ignore_patterns=["temp"])

def test_fromglob_missing_file_handler_in_enter():
    """
    Test that FromGlob raises a ValueError when entering the context and `file_handler` is missing.
    """
    folder_path = pathlib.Path("/fake/folder")

    with pytest.raises(ValueError, match=f"Glob folder_path {folder_path} does not exist."):
        from_glob = FromGlob(folder_path)

