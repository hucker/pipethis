import dataclasses

from _base import StreamItem

@dataclasses.dataclass
class LineStreamItem(StreamItem):
    """
    Represents a single line of text read from a (typically) a file in a pipeline.

    Attributes:
        sequence_id (int): Line number in the originating text resource.
        resource_name (str): Name of the source (e.g., file name).
        data (str): The textual content of the line.
    """
    data: str  # Overrides the `data` field with specific type

    def validate(self):

        # Validate `data` specific to `LineStreamItem`
        if not isinstance(self.data, str):
            raise ValueError("Data must be a string for LineStreamItem")


