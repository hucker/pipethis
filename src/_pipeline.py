from typing import List

from _base import InputBase, OutputBase, TransformBase
from _lineinfo import LineInfo


class Pipeline:
    def __init__(self):
        self.inputs: List[InputBase] = []
        self.transforms: List[TransformBase] = []
        self.outputs: List[OutputBase] = []

    def add_input(self, input_: InputBase):
        self.inputs.append(input_)
        return self

    def add_transform(self, transform: TransformBase):
        self.transforms.append(transform)
        return self

    def add_output(self, output: OutputBase):
        self.outputs.append(output)
        return self

    def __or__(self, other):
        if isinstance(other, InputBase):
            self.inputs.append(other)
        elif isinstance(other, TransformBase):
            self.transforms.append(other)
        elif isinstance(other, OutputBase):
            self.outputs.append(other)
        else:
            raise TypeError(f"Unsupported type for pipeline: {type(other).__name__}")
        return self

    def __ior__(self, other):
        return self.__or__(other)  # |= is the same as |

    def run(self):
        # Stream from inputs
        stream = (lineinfo for input_ in self.inputs for lineinfo in input_.stream())

        # Apply all transforms in sequence using a generator function
        stream = self._apply_transforms_in_sequence(stream)

        # Write to outputs
        for lineinfo in stream:
            for output in self.outputs:
                output.write(lineinfo)

        # Close all the outputs.
        for output in self.outputs:
            try:
                output.close()
            except IOError as ioex:
                print(f"IOError: {ioex} in pipeline output for output: {type(output).__name__}")
                pass

    def _apply_transforms_in_sequence(self, input_stream):
        """
        Apply all transforms in sequence to the input stream.
        This maintains the generator pattern without materializing a list.
        """
        stream = input_stream
        for transform in self.transforms:
            stream = self._apply_single_transform(stream, transform)
        return stream

    def _apply_single_transform(self, input_stream, transform):
        """
        Apply a single transform to all items in the input stream.
        """
        for lineinfo in input_stream:
            yield from transform.transform(lineinfo)
