import pytest

from _inputs import FromString
from _output import ToString
from _transform import AddMetaData, UpperCase,PassThrough
from _pipeline import Pipeline

@pytest.mark.pipeline
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
    assert output_to_string.size == 34

@pytest.mark.pipeline
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
    pipeline.add_transform(transform_to_upper)
    pipeline.add_transform(add_meta)
    pipeline.add_output(output_to_string)

    # Act: Run the pipeline
    pipeline.run()

    # Assert: Validate that the output matches expectations
    expected_output = "text:1first line\ntext:2second line\text:3third_line\n"
    assert output_to_string.text_output == expected_output


# Unit Tests
def test_pipeline_operator_overloading():
    # Create pipeline
    pipeline = Pipeline()

    # Create instances of the components
    from_string = FromString(text="foo")
    pass_through = PassThrough()
    to_string = ToString()

    # Build the pipeline
    pipeline = pipeline | from_string | pass_through | to_string

    # Verify the pipeline structure
    assert pipeline.inputs == [from_string], "Input not added correctly to the pipeline"
    assert pipeline.transforms == [pass_through], "Transform not added correctly to the pipeline"
    assert pipeline.outputs == [to_string], "Output not added correctly to the pipeline"


# Unit Tests
def test_pipeline_operator_overloading():
    # Create pipeline
    pipeline = Pipeline()

    # Create instances of the components
    from_string = FromString(text="foo")
    pass_through = PassThrough()
    to_string = ToString()

    # Build the pipeline
    pipeline = pipeline | from_string | pass_through | to_string

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

    # Normally the output of a pipleine ends up in a file.  In this case
    # the only output is a string output.  So when the pipeline finishes the
    # result is in the text_output property of the to_string object.
    assert to_string.text_output == "foo\n"
    assert to_string.size == 4