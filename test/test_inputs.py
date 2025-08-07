import pytest
import pathlib
# noinspection PyProtectedMember
from pipethis._input_from_string import FromString
from pipethis._input_from_folder import FromFolder
from pipethis._input_from_file import FromFile
from pipethis._input_from_strings import FromStrings
from pipethis._base import InputBase
# noinspection PyProtectedMember
from pipethis._file_handler import TextFileHandler,FileHandlerBase
from pipethis._streamitem import LineStreamItem
from pipethis._base import InputBase

@pytest.mark.parametrize(
    "items, sep, expected, should_raise",
    [
        # Test cases where no exception is expected
        (None, " ", [], None),  # None input -> empty list
        ("", " ", [], None),  # Empty string -> empty list
        ("apple banana cherry", " ", ["apple", "banana", "cherry"], None),  # String with default separator
        ("apple,banana,cherry", ",", ["apple", "banana", "cherry"], None),  # String with custom separator
        (["apple", "banana", "cherry"], " ", ["apple", "banana", "cherry"], None),  # Valid list of strings
        ([], " ", [], None),  # Empty list -> empty list
        (["", "apple", ""], " ", ["", "apple", ""], None),  # List with empty strings -> unchanged

        # Test cases where exceptions are expected
        (["apple", 123, "cherry"], " ", None, ValueError),  # List with a non-string element -> raises ValueError
        (123, " ", None, ValueError),  # Invalid type (integer) -> raises ValueError
    ],
)
def test_list_or_string(items, sep, expected, should_raise):
    """
    Parameterized test for the `_list_or_string` method using a DummyInputBase.
    Covers both valid inputs and exceptions.
    """

    class DummyInputBase(InputBase):
        """Concrete subclass of InputBase with all abstract methods implemented for testing purposes."""

        def stream(self):
            """Dummy implementation of the abstract 'stream' method."""


    dummy = DummyInputBase()
    if should_raise:
        with pytest.raises(should_raise):
            dummy._list_or_string(items, sep)
    else:
        assert dummy._list_or_string(items, sep) == expected

def test_from_string_list():
    """Test `FromStrings` with a single-line input."""
    lines = ["This is the first line.","This is the next line"]
    from_strings = FromStrings(lines,sep='\n',name="text")
    results = from_strings.to_list()

    assert len(results) == 2
    assert results[0].sequence_id == 1
    assert results[0].data == "This is the first line."
    assert results[1].sequence_id == 1
    assert results[1].data == "This is the next line"


def test_from_string_list_double_line():
    """Test `FromStrings` with a multi line multi input."""
    lines = ["This is the\nfirst line.","This is the\nnext line"]
    from_strings = FromStrings(lines,sep='\n',name="text")
    results = from_strings.to_list()

    assert len(results) == 4
    assert results[0].sequence_id == 1
    assert results[0].data == "This is the"
    assert results[1].sequence_id == 2
    assert results[1].data == "first line."

    assert results[2].sequence_id == 1
    assert results[2].data == "This is the"
    assert results[3].sequence_id == 2
    assert results[3].data == "next line"



def test_from_strings_single_line():
    """Test `FromString` with a single-line input."""
    lines = ["This is a single-line test."]
    from_strings = FromStrings(lines,sep='\n',name="text")
    results = from_strings.to_list()

    assert len(results) == 1
    assert results[0].sequence_id == 1



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
    results = from_strings.to_list()

    # Assert that the length of the results matches the expected results length
    assert len(results) == len(expected_results)

    # Verify sequence_id and data for each result if results are not empty
    for result, (expected_sequence_id, expected_data) in zip(results, expected_results):
        assert result.sequence_id == expected_sequence_id
        assert result.data == expected_data





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




