from contextlib import ExitStack
from typing import List

# Nice that pipeline only operates on baseclasses.
from _base import InputBase, OutputBase, TransformBase


class Pipeline:
    """
    A configurable data processing pipeline that streams data from multiple inputs,
    applies a sequence of transformations, and writes the processed data to multiple outputs.

    The pipeline operates using `InputBase`, `TransformBase`, and `OutputBase`-derived objects:
      - **Inputs**: Data sources that produce streams of `LineStreamItem` objects (e.g., files, strings).
      - **Transforms**: Processors that modify the `LineStreamItem` objects in sequence.
      - **Outputs**: Data targets for writing transformed rows.

    Resource handling (e.g., cleaning up files, closing connections) is managed via context managers,
    utilizing `ExitStack`.

    Attributes:
        inputs (List[InputBase]):
            A list of input sources that implement the `InputBase` interface, each emitting a
            stream of `LineStreamItem` objects.
        transforms (List[TransformBase]):
            A list of transformations implementing the `TransformBase` interface, to be applied
            sequentially to each `LineStreamItem` object in the input stream.
        outputs (List[OutputBase]):
            A list of output targets implementing the `OutputBase` interface, each writing the
            final transformed stream of `LineStreamItem` objects.
    """

    def __init__(self):
        """
        Initializes an empty pipeline with no inputs, transforms, or outputs.
        """
        self.inputs: List[InputBase] = []
        self.transforms: List[TransformBase] = []
        self.outputs: List[OutputBase] = []
        self.pipelines: List["Pipeline"] = []

    def add_pipeline(self, input_: "Pipeline"):
        """
        Adds sub pipeline to the pipeline.


        Args:
            input_ (InputBase):
                An instance of a class derived from `InputBase` that defines a data source
                (e.g., a file reader or in-memory stream).

        Returns:
            Pipeline:
                The current pipeline instance (to allow method chaining).

        """
        self.pipelines.append(input_)
        return self

    def add_input(self, input_: InputBase):
        """
        Adds an input source to the pipeline.

        Args:
            input_ (InputBase):
                An instance of a class derived from `InputBase` that defines a data source
                (e.g., a file reader or in-memory stream).

        Returns:
            Pipeline:
                The current pipeline instance (to allow method chaining).
        """
        self.inputs.append(input_)
        return self

    def add_transform(self, transform: TransformBase) -> "Pipeline":
        """
        Adds a transformation to the pipeline.

        Args:
            transform (TransformBase):
                An instance of a class derived from `TransformBase` that defines a transform
                to process `LineStreamItem` objects.

        Returns:
            Pipeline:
                The current pipeline instance (to allow method chaining).
        """
        self.transforms.append(transform)
        return self

    def add_output(self, output: OutputBase) -> "Pipeline":
        """
        Adds an output target to the pipeline.

        Args:
            output (OutputBase):
                An instance of a class derived from `OutputBase` that defines a data target
                (e.g., a file writer or console printer).

        Returns:
            Pipeline:
                The current pipeline instance (to allow method chaining).
        """
        self.outputs.append(output)
        return self

    def __or__(self, other: InputBase | TransformBase | OutputBase) -> "Pipeline":
        """
        Overloads the `|` operator to add an input, transform, or output to the pipeline.

        Args:
            other (Union[InputBase, TransformBase, OutputBase]):
                The component to add to the pipeline. Must be an `InputBase`, `TransformBase`,
                or `OutputBase` instance.

        Returns:
            Pipeline:
                The modified pipeline instance.

        Raises:
            TypeError:
                If the provided `other` is not a valid pipeline component.
        """
        if isinstance(other, InputBase):
            self.inputs.append(other)
        elif isinstance(other, TransformBase):
            self.transforms.append(other)
        elif isinstance(other, OutputBase):
            self.outputs.append(other)
        else:
            raise TypeError(f"Unsupported type for pipeline: {type(other).__name__}")
        return self

    def __ior__(self, other: InputBase | TransformBase | OutputBase) -> "Pipeline":
        """
        Overloads the `|=` operator, delegating to the `|` operator.

        Args:
            other (Union[InputBase, TransformBase, OutputBase]):
                The component to add to the pipeline.

        Returns:
            Pipeline:
                The modified pipeline instance.
        """
        return self.__or__(other)  # |= behaves the same as |

    def run(self) -> "Pipeline":
        """
        Executes the pipeline by:
        1. Managing context for inputs and outputs using `ExitStack`.
        2. Streaming data from each input as `LineStreamItem` objects.
        3. Applying the defined sequence of transformations to each `LineStreamItem` object.
        4. Writing the transformed data to all outputs.

        Errors encountered during processing are not explicitly handled here;
        they will propagate unless caught elsewhere.

        Raises:
            Exception:
                If there are errors in reading inputs, applying transforms, or writing outputs.

        Steps:
            - Each input and output is properly managed as a context, ensuring cleanup.
            - Each transformation is applied to the data stream one row at a time.
        """

        # Thank you GPT-4, I guessed that there was some mechanism for handling multiple
        # context managers at the same time.  This basically worked out of the box.
        with ExitStack() as stack:
            # Enter context for inputs and outputs
            managed_inputs = [stack.enter_context(input_file) for input_file in self.inputs]
            managed_outputs = [stack.enter_context(output) for output in self.outputs]

            # Stream data from inputs
            stream = (
                row
                for input_file in managed_inputs
                for row in input_file.stream()
            )

            # Apply all transforms to each row in sequence
            stream = self._apply_transforms_in_sequence(stream)

            # Write the final transformed rows to all outputs
            for row in stream:
                for output in managed_outputs:
                    output.write(row)

        return self

    def _apply_transforms_in_sequence(self, input_stream):
        """
        Applies all transformations to a stream of `LineStreamItem` objects in sequence.

        Args:
            input_stream (iterable):
                A generator or other iterable that emits `LineStreamItem` objects.

        Returns:
            generator:
                A generator that yields transformed `LineStreamItem` objects.
        """
        stream = input_stream
        for transform in self.transforms:
            stream = self._apply_single_transform(stream, transform)
        return stream

    def _apply_single_transform(self, input_stream, transform):
        """
        Applies a single transformation function to an input stream.

        Args:
            input_stream (iterable):
                A generator or other iterable emitting `LineStreamItem` objects.
            transform (TransformBase):
                An instance of a class derived from `TransformBase` that defines a
                `transform(row)` method.

        Yields:
            LineStreamItem:
                The transformed `LineStreamItem` objects, one at a time.

        Raises:
            Exception:
                If the transform's `transform()` method throws an error for any row.
        """
        for lineinfo in input_stream:
            yield from transform.transform(lineinfo)
