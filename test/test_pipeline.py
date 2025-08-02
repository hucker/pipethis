import pytest

# noinspection PyProtectedMember
from pipethis._input_from_string import FromString
# noinspection PyProtectedMember
from pipethis._output_to_string import ToString
# noinspection PyProtectedMember
from pipethis._transform import AddMetaData, UpperCase,PassThrough,RegexKeepFilter
# noinspection PyProtectedMember
from pipethis._pipeline import Pipeline

def test_pipeline_with_string_io():
    # Arrange: Define input, transformations, output, and the pipeline
    input_data = "first line\nsecond line\nthird line"
    input_from_string = FromString(input_data)
    transform_to_upper = UpperCase()
    output_to_string = ToString()

    # Create and configure the pipeline
    pipeline = Pipeline()
    pipeline.add_input(input_from_string)
    pipeline.add_transform(transform_to_upper)
    pipeline.add_output(output_to_string)

    # Act: Run the pipeline
    pipeline.run()

    # Assert: Validate that the output matches expectations
    expected_output = "FIRST LINE\nSECOND LINE\nTHIRD LINE\n"
    assert output_to_string.text_output == expected_output

def test_pipeline_with_meta_string_io():
    # Arrange: Define input, transformations, output, and the pipeline
    input_data = "first line\nsecond line\nthird line"
    input_from_string = FromString(input_data)
    transform_to_upper = UpperCase()
    output_to_string = ToString()
    add_meta = AddMetaData()

    # Create and configure the pipeline
    pipeline = Pipeline()
    pipeline.add_input(input_from_string)
    pipeline.add_transform(add_meta)
    pipeline.add_transform(transform_to_upper)
    pipeline.add_output(output_to_string)

    # Act: Run the pipeline
    pipeline.run()

    # Assert: Validate that the output matches expectations
    expected_output = "TEXT:1:FIRST LINE\nTEXT:2:SECOND LINE\nTEXT:3:THIRD LINE\n"
    assert output_to_string.text_output == expected_output


# Unit Tests
def test_pipeline_operator_overloading():

    # Create instances of the components
    from_string = FromString(text="foo")
    pass_through = PassThrough()
    to_string = ToString()

    # Build the pipeline
    pipeline =  from_string | pass_through | to_string

    # Verify the pipeline structure
    assert pipeline.inputs == [from_string], "Input not added correctly to the pipeline"
    assert pipeline.transforms == [pass_through], "Transform not added correctly to the pipeline"
    assert pipeline.outputs == [to_string], "Output not added correctly to the pipeline"


def test_pipeline_preserves_order():
    # Create pipeline
    pipeline = Pipeline()

    # Create multiple components
    from_string1 = FromString(text="foo")
    from_string2 = FromString(text="foo")
    pass_through1 = PassThrough()
    pass_through2 = PassThrough()
    to_string1 = ToString()
    to_string2 = ToString()

    # Build the pipeline
    pipeline = pipeline | from_string1 | pass_through1 | to_string1
    pipeline = pipeline | from_string2 | pass_through2 | to_string2

    # Verify that elements are added in the correct order
    assert pipeline.inputs == [from_string1, from_string2], "Inputs order is incorrect"
    assert pipeline.transforms == [pass_through1, pass_through2], "Transforms order is incorrect"
    assert pipeline.outputs == [to_string1, to_string2], "Outputs order is incorrect"


def test_pipeline_ordering():
    pipeline = Pipeline()

    # Attempting to add invalid
    from_string1 =  FromString("foo")
    from_string2 =  FromString("fum")
    pass_through1 = PassThrough()
    pass_through2 = PassThrough()
    to_string1 = ToString()
    to_string2 = ToString()

    # Build the pipeline
    pipeline = pipeline | from_string1 | pass_through1 | to_string1
    pipeline = pipeline | from_string2 | pass_through2 | to_string2

    # Verify that elements are added in the correct order
    assert pipeline.inputs == [from_string1, from_string2], "Inputs order is incorrect"
    assert pipeline.transforms == [pass_through1, pass_through2], "Transforms order is incorrect"
    assert pipeline.outputs == [to_string1, to_string2], "Outputs order is incorrect"


def test_pipeline_invalid_element():
    pipeline = Pipeline()

    # Attempting to add invalid components should raise a TypeError
    with pytest.raises(TypeError, match="Unsupported type for pipeline"):
        pipeline = pipeline | "InvalidElement"

    with pytest.raises(TypeError, match="Unsupported type for pipeline"):
        pipeline = pipeline | 123

def test_pipeline_ior_operator():
    """Test the functionality of __ior__ (|= operator) in the Pipeline class."""

    # Arrange
    pipeline = Pipeline()
    to_string = ToString()
    # Act
    pipeline |= FromString("foo")
    pipeline |= PassThrough()
    pipeline |= to_string

    # Assert
    # Check that all components are properly added to the pipeline
    assert len(pipeline.inputs) == 1
    assert isinstance(pipeline.inputs[0], FromString)

    assert len(pipeline.transforms) == 1
    assert isinstance(pipeline.transforms[0], PassThrough)

    assert len(pipeline.outputs) == 1
    assert isinstance(pipeline.outputs[0], ToString)

    pipeline.run()

    # Normally the output of a pipleline ends up in a file.  In this case
    # the only output is a string output.  So when the pipeline finishes the
    # result is in the text_output property of the to_string object.
    assert to_string.text_output == "foo\n"

def test_pipeline_with_error_filter():
    """
    Test a pipeline that filters to keep only lines containing 'ERROR'.
    Verifies that the filter correctly processes the input stream.
    """
    # Input data with a mix of error and non-error lines
    input_data = """This is a normal log line
This line has an ERROR message
Another normal informational line
CRITICAL ERROR: system failure
Just a debug message
ERROR: could not connect to database"""

    # Set up pipeline components
    input_handler = FromString(input_data)
    output_handler = ToString()
    error_filter = RegexKeepFilter(r".*ERROR.*")

    # Create and run the pipeline
    pipeline = Pipeline()
    pipeline.add_input(input_handler)
    pipeline.add_transform(error_filter)
    pipeline.add_output(output_handler)
    pipeline.run()

    # Get the filtered output
    result = output_handler.text_output

    # Split the result into lines for easier verification
    result_lines = result.strip().split('\n')

    # Verify only ERROR lines were kept
    assert len(result_lines) == 3
    assert "This line has an ERROR message" in result_lines
    assert "CRITICAL ERROR: system failure" in result_lines
    assert "ERROR: could not connect to database" in result_lines

    # Verify non-error lines were filtered out
    assert "This is a normal log line" not in result_lines
    assert "Another normal informational line" not in result_lines
    assert "Just a debug message" not in result_lines


def test_pipeline_construction_methods_comparison():
    """
    Test that both pipeline construction methods (manual and pipe operator)
    produce identical results when filtering for ERROR lines.
    """
    # Input data with a mix of error and non-error lines
    input_data = """This is a normal log line
This line has an ERROR message
Another normal informational line
CRITICAL ERROR: system failure
Just a debug message
ERROR: could not connect to database"""

    # Set up pipeline components for manual construction
    input_handler1 = FromString(input_data)
    output_handler1 = ToString()
    error_filter1 = RegexKeepFilter(r".*ERROR.*")

    # Create and run the manual pipeline
    manual_pipeline = Pipeline()
    manual_pipeline.add_input(input_handler1)
    manual_pipeline.add_transform(error_filter1)
    manual_pipeline.add_output(output_handler1)
    manual_pipeline.run()

    # Set up components for pipe operator construction
    pipe_pipeline = Pipeline()
    input_handler2 = FromString(input_data)
    output_handler2 = ToString()
    error_filter2 = RegexKeepFilter(r".*ERROR.*")

    # Create and run the pipeline using pipe operators
    pipe_pipeline = pipe_pipeline | input_handler2 | error_filter2 | output_handler2
    pipe_pipeline.run()

    # Get results from both pipelines
    manual_result = output_handler1.text_output
    pipe_result = output_handler2.text_output

    # Verify both pipelines produced identical results
    assert manual_result == pipe_result

    # Verify the filtering worked correctly in both cases
    result_lines = manual_result.strip().split('\n')
    assert len(result_lines) == 3
    assert "This line has an ERROR message" in result_lines
    assert "CRITICAL ERROR: system failure" in result_lines
    assert "ERROR: could not connect to database" in result_lines

def test_multi_transform_pipeline_construction_comparison():
    """
    Test that both pipeline construction methods produce identical results
    with multiple transforms (filtering for ERROR lines containing 'database').
    """
    # Input data with various error types
    input_data = """ERROR: network timeout
WARNING: disk space low
ERROR: could not connect to database
ERROR: invalid user credentials
System running normally
ERROR: database query failed"""

    # Set up pipeline components for manual construction
    input_handler1 = FromString(input_data)
    output_handler1 = ToString()
    error_filter1 = RegexKeepFilter(r".*ERROR.*")
    database_filter1 = RegexKeepFilter(r".*database.*")

    # Create and run the manual pipeline
    manual_pipeline = Pipeline()
    manual_pipeline.add_input(input_handler1)
    manual_pipeline.add_transform(error_filter1)
    manual_pipeline.add_transform(database_filter1)
    manual_pipeline.add_output(output_handler1)
    manual_pipeline.run()

    # Set up components for pipe operator construction
    input_handler2 = FromString(input_data)
    output_handler2 = ToString()
    error_filter2 = RegexKeepFilter(r".*ERROR.*")
    database_filter2 = RegexKeepFilter(r".*database.*")

    # Create and run the pipeline using pipe operators
    pipe_pipeline = Pipeline()
    pipe_pipeline = pipe_pipeline | input_handler2 | error_filter2 | database_filter2 | output_handler2
    pipe_pipeline.run()

    # Get results from both pipelines
    manual_result = output_handler1.text_output
    pipe_result = output_handler2.text_output

    # Verify both pipelines produced identical results
    assert manual_result == pipe_result

    # Verify the filtering worked correctly
    result_lines = manual_result.strip().split('\n')
    assert len(result_lines) == 2
    assert "ERROR: could not connect to database" in result_lines
    assert "ERROR: database query failed" in result_lines

def test_implicit_pipeline():
    """
    Verify that you can create a pipeline when the first item in a chain
    is derived from InputBase.  This saves typing Pipeline() at the start
    of all pipelines.
    """

    input_data = """ERROR: network timeout
WARNING: disk space low
ERROR: could not connect to database
ERROR: invalid user credentials
System running normally
ERROR: database query failed"""

    # Set up components for pipe operator construction
    input_handler = FromString(input_data)
    output_handler = ToString()
    error_filter = RegexKeepFilter(r".*ERROR.*")
    database_filter = RegexKeepFilter(r".*database.*")

    # no manually created Pipeline() to start the pipeline
    pipe_pipeline = input_handler | error_filter | database_filter | output_handler
    result = pipe_pipeline.run()
    assert output_handler.text_output == "ERROR: could not connect to database\nERROR: database query failed\n"


def test_add_pipeline():
    pipeline1 = Pipeline() | FromString("foo")
    pipeline2 = Pipeline() | FromString("baz")
    pipeline3 = Pipeline()
    pipeline3.add_pipeline(pipeline1)
    pipeline3.add_pipeline(pipeline2)
    assert pipeline3.pipelines == [pipeline1, pipeline2]