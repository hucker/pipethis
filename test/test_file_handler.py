import pathlib
import pytest
from pipethis import TextFileHandler, LineStreamItem  # Adjust import paths as necessary



def test_text_file_handler_stream(tmp_path:pathlib.Path):
    """
    Test the `TextFileHandler` class to ensure it streams lines correctly
    and manages file resources properly.
    """
    # Create a temporary .txt file for testing
    test_file = tmp_path / "test_file.txt"
    content = "line 1\nline 2\nline 3\nline 4\n"
    test_file.write_text(content)

    # Use TextFileHandler within a context manager
    with TextFileHandler(test_file) as handler:
        results = list(handler.stream())

    # Assert the number of lines matches the file content
    assert len(results) == 4, "The file_handler should process all lines in the file."

    # Verify the sequence IDs and content of each line
    expected_data = ["line 1", "line 2", "line 3", "line 4"]
    for index, (result, expected_line) in enumerate(zip(results, expected_data), start=1):
        assert isinstance(result, LineStreamItem), f"Result {result} must be a LineStreamItem."
        assert result.sequence_id == index, f"Sequence ID should be {index}, got {result.sequence_id}."
        assert result.data == expected_line, f"Expected line '{expected_line}', got '{result.data}'."

    # Ensure the file has been properly closed after exit
    assert handler._file is None, "The file should be properly closed after exiting the context manager."


def test_text_file_handler_without_context_manager(tmp_path:pathlib.Path):
    """
    Test the `TextFileHandler` raises an error if used outside a context manager.
    """
    # Create a temporary .txt file
    test_file = tmp_path / "test_file.txt"
    content = "line 1\nline 2\nline 3\n"
    test_file.write_text(content)

    # Instantiate the file_handler without entering a context manager
    handler = TextFileHandler(test_file)

    # Ensure it raises a RuntimeError when calling stream
    with pytest.raises(RuntimeError, match="The file is not open. You must use this file_handler in a context manager."):
        list(handler.stream())

def test_text_file_handler_stream_for_loop(tmp_path:pathlib.Path):
    """
    Test the `TextFileHandler.stream` method using a for loop
    to ensure it streams lines correctly and aligns sequence IDs with line content.
    """
    # Create a temporary .txt file for testing
    test_file = tmp_path / "test_file.txt"
    content = "line 1\nline 2\nline 3\nline 4\n"
    test_file.write_text(content)

    expected_data = ["line 1", "line 2", "line 3", "line 4"]

    # Use TextFileHandler within a context manager
    with TextFileHandler(test_file) as handler:
        # Iterate over the stream method directly in a for loop
        for sequence_id, (expected_line, item) in enumerate(zip(expected_data, handler.stream()), start=1):
            # Assert that the returned object is an instance of LineStreamItem
            assert isinstance(item, LineStreamItem), f"Item {item} must be a LineStreamItem."
            # Assert that the sequence_id matches
            assert item.sequence_id == sequence_id, f"Expected sequence ID {sequence_id}, got {item.sequence_id}."
            # Assert that the content matches
            assert item.data == expected_line, f"Expected line '{expected_line}', got '{item.data}'."

    # Closed file should be None
    assert handler._file is None, "The file should be properly closed after exiting the context manager."

def test_text_file_handler_non_existent_file(tmp_path: pathlib.Path):
    """
    Verify that TextFileHandler raises an exception when the file does not exist.
    """
    # Create a path for a non-existent file
    non_existent_file = tmp_path / "non_existent_file.txt"

    # Expect the TextFileHandler to raise an exception when trying to access a non-existent file
    with pytest.raises(FileNotFoundError, match=str(non_existent_file)):
        with TextFileHandler(non_existent_file) as handler:
            # Attempting to stream should never occur, as the exception should be raised earlier
            list(handler.stream())

