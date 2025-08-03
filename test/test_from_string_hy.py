from pipethis._input_from_string import FromString  # Adjust to your module's structure
from hypothesis import given, strategies as st


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