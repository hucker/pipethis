import dataclasses

from _base import DataInfo

@dataclasses.dataclass
class LineInfo(DataInfo):
    """
    Represents a single line of text in a pipeline.

    Attributes:
        sequence_id (int): Line number in the originating text resource.
        resource_name (str): Name of the source (e.g., file name).
        data (str): The textual content of the line.
    """
    data: str  # Overrides the `data` field with specific type

    def validate(self):

        # Validate `data` specific to `LineInfo`
        if not isinstance(self.data, str):
            raise ValueError("Data must be a string for LineInfo")


