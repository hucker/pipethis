import pytest
import pathlib
# noinspection PyProtectedMember
from pipethis._input_from_string import FromString
from pipethis._input_from_folder import FromFolder
from pipethis._input_from_file import FromFile
from pipethis._input_from_glob import FromGlob
from pipethis._input_from_strings import FromStrings

# noinspection PyProtectedMember
from pipethis._file_handler import TextFileHandler,FileHandlerBase
from pipethis._streamitem import LineStreamItem





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

def test_from_strings_single_line():
    """Test `FromString` with a single-line input."""
    lines = ["This is a single-line test."]
    from_strings = FromStrings(lines,sep='\n',name="text")
    results = list(from_strings.stream())

    assert len(results) == 1
    assert results[0].sequence_id == 1


def test_from_string_list():
    """Test `FromString` with a single-line input."""
    lines = ["This is the first line.","This is the next line"]
    from_strings = FromStrings(lines,sep='\n',name="text")
    results = list(from_strings.stream())

    assert len(results) == 2
    assert results[0].sequence_id == 1
    assert results[0].data == "This is the first line."
    assert results[1].sequence_id == 1
    assert results[1].data == "This is the next line"


def test_from_string_list_double_line():
    """Test `FromString` with a multi line multi input."""
    lines = ["This is the\nfirst line.","This is the\nnext line"]
    from_strings = FromStrings(lines,sep='\n',name="text")
    results = list(from_strings.stream())

    assert len(results) == 4
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

    from_strings = FromStrings(lines, sep='\n', name="text")
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





# Pytest fixture to create temporary files for testing
@pytest.fixture
def sample_file(tmp_path):
    """Creates a sample temporary file for testing."""
    file_path = tmp_path / "test_file.txt"
    content = "Hello\nWorld\nThis is a test\nLine 4"
    file_path.write_text(content)
    return file_path


def test_from_file_context_manager(sample_file):
    """Test `FromFile` with a basic file."""
    with FromFile(sample_file, handler=TextFileHandler) as from_file:
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

def test_from_file_stream(sample_file):
    """Test `FromFile` stream without a context manager."""

    ff = FromFile(sample_file, handler=TextFileHandler)
    results = list(ff.stream())

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

def test_from_file_file_with_trailing_newlines(tmp_path):
    """Test `FromFile` with a file that has trailing newlines."""
    file_with_trailing_newlines = tmp_path / "trailing_newlines.txt"
    content = "Line 1\nLine 2\n\n"
    file_with_trailing_newlines.write_text(content)

    with FromFile(file_with_trailing_newlines) as from_file:
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

    with FromFile(unicode_file) as from_file:
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



def test_from_folder_conflicting_filters():
    """Test that a ValueError is raised when conflicting filters are set."""
    with pytest.raises(ValueError, match="You can specify either keep_patterns or ignore_patterns, but not both."):
        FromFolder(
            folder_path="/path/to/folder",
            keep_patterns=["*.txt"],
            ignore_patterns=["*.txt"]  # Conflict with `keep_patterns`
        )

def test_from_strings_context_manager_and_iteration():
    """
    Test using FromStrings as a context manager with iteration over its streamed lines.
    Verify that lines are properly processed and exited without issues.
    """
    lines = ["This is a test.", "Another example line."]
    separator = " "

    with FromStrings(lines, sep=separator, name="test") as fs:
        # Collect all items from the stream
        streamed_results = list(fs.stream())

        # Expected result
        expected_results = [
            LineStreamItem(sequence_id=1, resource_name="test-1", data="This"),
            LineStreamItem(sequence_id=2, resource_name="test-1", data="is"),
            LineStreamItem(sequence_id=3, resource_name="test-1", data="a"),
            LineStreamItem(sequence_id=4, resource_name="test-1", data="test."),
            LineStreamItem(sequence_id=1, resource_name="test-2", data="Another"),
            LineStreamItem(sequence_id=2, resource_name="test-2", data="example"),
            LineStreamItem(sequence_id=3, resource_name="test-2", data="line."),
        ]

        # Verify each LineStreamItem in the streamed results matches the expected ones
        assert len(streamed_results) == len(expected_results)
        for result, expected in zip(streamed_results, expected_results):
            assert result.sequence_id == expected.sequence_id
            assert result.resource_name == expected.resource_name
            assert result.data == expected.data

def test_from_file_empty_file(tmp_path):
    """Test `FromFile` with an empty file."""
    empty_file = tmp_path / "empty_file.txt"
    empty_file.write_text("")  # Create an empty file

    # Test using the file handler within a context manager
    with FromFile(empty_file) as from_file:
        # Get all streamed items
        result = list(from_file.stream())
        assert len(result) == 0

def test_decorator_register_handler(tmp_path):
    """
    Test that the register_handler decorator correctly registers a handler for a file extension (.log)
    and that the handler is properly used in streaming a file's content as LineStreamItem objects.
    """

    # Ensure the registry is clean before starting (for isolation in tests)
    FromFile.clear_registered_handlers()

    # Register a custom handler for ".log" files
    @FromFile.register_handler(".log")
    class LogFileHandler(FileHandlerBase):
        def __init__(self, file_path: pathlib.Path):
            super().__init__(file_path)
            self._file = None

        def __enter__(self):
            self._file = self.file_path.open("r", encoding="utf-8")
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            if self._file:
                self._file.close()

        def stream(self):
            if not self._file:
                raise RuntimeError("File not open. Use in a context manager.")
            for idx, line in enumerate(self._file, start=1):
                # Yield a LineStreamItem for each line
                yield LineStreamItem(sequence_id=idx, resource_name=str(self.file_path), data=line.strip())

    # Verify that the handler is now in the _HANDLER_MAP
    assert FromFile.get_registered_handler(".log") == LogFileHandler
    assert ".log" in FromFile.get_all_registered_handlers()

    # Test it with a sample .log file
    test_file = tmp_path / "example.log"
    test_file.write_text("This is a log line.\nAnother log line.\n", encoding="utf-8")

    # Create FromFile instance
    from_file_instance = FromFile(filepath=str(test_file))

    # Check that the correct handler is resolved and used
    assert isinstance(from_file_instance.file_handler, LogFileHandler)
    assert from_file_instance.file_handler.file_path == test_file

    # Use the resolved handler within the context manager
    with from_file_instance as handler:
        assert isinstance(handler, LogFileHandler)
        assert handler.file_path == test_file

        # Stream and verify the content
        result = list(handler.stream())
        assert len(result) == 2

        # Verify the content of the yielded LineStreamItems
        assert result[0].sequence_id == 1
        assert result[0].resource_name == str(test_file)
        assert result[0].data == "This is a log line."

        assert result[1].sequence_id == 2
        assert result[1].resource_name == str(test_file)
        assert result[1].data == "Another log line."



def test_register_handler_invalid_extension():
    """
    Test that a ValueError is raised when registering an invalid file extension that
    does not start with a dot.
    """
    with pytest.raises(ValueError, match="Invalid extension pattern 'log'. Must start with '.'"):
        @FromFile.register_handler("log")
        class InvalidExtensionHandler(FileHandlerBase): # pragma no cover
            pass


def test_register_handler_duplicate_registration_without_force():
    """
    Test that a ValueError is raised when attempting to register a handler for an
    already-registered extension without `force=True`.
    """

    @FromFile.register_handler(".log")
    class LogHandler(FileHandlerBase):
        pass

    with pytest.raises(ValueError,
                       match="Handler for extension '.log' is already registered. Use force=True to overwrite."):
        @FromFile.register_handler(".log")
        class AnotherLogHandler(FileHandlerBase):
            pass


def test_register_handler_with_force_overwrites():
    """
    Test that using `force=True` allows overwriting an already-registered handler.
    """

    @FromFile.register_handler(".log")
    class LogHandler(FileHandlerBase):
        pass

    # Overwrite with force
    @FromFile.register_handler(".log", force=True)
    class AnotherLogHandler(FileHandlerBase):
        pass

    assert FromFile.get_registered_handler(".log") == AnotherLogHandler


def test_get_registered_handler_no_handler_exists():
    """
    Test that `get_registered_handler` returns None for nonexistent extensions.
    """
    assert FromFile.get_registered_handler(".nonexistent") is None


def test_clear_registered_handlers():
    """
    Test that `clear_registered_handlers` successfully removes all registered handlers.
    """

    @FromFile.register_handler(".log")
    class LogHandler(FileHandlerBase):
        pass

    assert ".log" in FromFile.get_all_registered_handlers()

    FromFile.clear_registered_handlers()

    assert FromFile.get_all_registered_handlers() == {}


def test_file_handler_resolution_with_invalid_extension():
    """
    Test that a default handler (TextFileHandler) is provided for unregistered extensions.
    """

    @FromFile.register_handler(".log")
    class LogHandler(FileHandlerBase):
        pass

    # Use a file with an unregistered extension
    handler = FromFile.get_registered_handler(".txt")

    assert handler is None  # Default to TextFileHandler if implemented


def test_register_handler_invalid_extension_pattern():
    """
    Test that a ValueError is raised when registering an invalid extension pattern.
    """
    with pytest.raises(ValueError, match="Invalid extension pattern 'invalid_pattern'"):
        @FromFile.register_handler("invalid_pattern")
        class InvalidHandler(FileHandlerBase): # pragma no cover
            ...
