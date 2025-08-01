"""
This module defines data structures for encapsulating items in a data pipeline.

The primary purpose of the classes in this module is to represent various types of data
(such as lines of text or images) flowing through a pipeline. These classes extend
the `StreamItem` base class and provide validations specific to their respective data types.

Classes:
    - LineStreamItem: Represents a single line of text in a pipeline. This is typically
      used for processing textual files, such as logs or structured text.
    - ImageStreamItem: Represents an image in a pipeline. This is designed for handling
      image processing tasks using the PIL library (Pillow).

Each class includes validation logic to ensure that the `data` being processed matches
the expected data type (e.g., string for `LineStreamItem` or `Image.Image` for `ImageStreamItem`).
"""


import dataclasses

from PIL import Image

from ._base import StreamItem


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


@dataclasses.dataclass
class ImageStreamItem(StreamItem):
    """
    Represents an image within a data pipeline.

    Attributes:
        sequence_id (int): Identifier for the item in the stream.
        resource_name (str): Name of the source resource (e.g., file name).
        data (Image.Image): The PIL image object.
    """
    data: Image.Image  # The PIL.Image object for this stream item.

    def validate(self):
        """Validate that data is an instance of PIL.Image.Image."""
        if not isinstance(self.data, Image.Image):
            raise ValueError("Data must be an instance of PIL.Image.Image for ImageStreamItem")
