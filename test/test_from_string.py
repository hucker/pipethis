


from pipethis._input_from_string import FromString

def test_from_string_single_line_input():
    """Test `FromString` with single-line input and newline separator."""
    lines = "This is the first line.\nThis is the next line"
    from_string_instance = FromString(lines, sep="\n", name="text")

    # Obtain results from the stream
    results = from_string_instance.to_list()

    # Verify the number of results
    assert len(results) == 2

    # Verify the content of each result
    assert results[0].sequence_id == 1
    assert results[0].data == "This is the first line."
    assert results[1].sequence_id == 2
    assert results[1].data == "This is the next line"


def test_from_string_comma_separator():
    """Test `FromString` with a comma separator."""
    lines = ["line1,line2,line3"]
    from_string_instance = FromString(lines[0], sep=",", name="text")

    # Obtain results from the stream
    results = from_string_instance.to_list()

    # Verify the number of results
    assert len(results) == 3

    # Verify the content of each result
    assert results[0].sequence_id == 1
    assert results[0].data == "line1"
    assert results[1].sequence_id == 2
    assert results[1].data == "line2"
    assert results[2].sequence_id == 3
    assert results[2].data == "line3"
