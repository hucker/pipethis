from pipethis._transform import LineStreamItem
from pipethis._input_from_string import FromString  # Adjust to your module's structure
from hypothesis import given, settings, Verbosity, strategies as st


# Define the shared alphabet
shared_alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890~!@#$%^&*()<>?:\"\n"



@settings(max_examples=1000,verbosity=Verbosity.verbose)
@given(
    text=st.text(min_size=0, max_size=500, alphabet=shared_alphabet),
    sep=st.text(min_size=1, max_size=10, alphabet=shared_alphabet)  # Use the same alphabet here
)
def test_from_string_stream(text, sep):
    # Create the FromString instance
    from_string = FromString(text, sep=sep, name="test_source")

    # Get the items from the stream
    stream_items = list(from_string.stream())

    # Manually split the text for comparison
    expected_lines = text.split(sep)

    # Assert that the number of items matches
    assert len(stream_items) == len(expected_lines)

    # Assert the content and metadata of each item
    for i, (expected, item) in enumerate(zip(expected_lines, stream_items), start=1):
        assert isinstance(item, LineStreamItem)
        assert item.sequence_id == i  # Line numbers should start at 1
        assert item.data == expected
        assert item.resource_name == "test_source"



