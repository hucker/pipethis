"""
This module provides a collection of xform classes for processing streams of text data.

Each transformation class implements the `TransformBase` interface and defines a `transform` method
that operates on `LineStreamItem` objects. These transformations can modify, filter, or enhance the
data as needed within a pipeline.

Available Transformations:
- `PassThrough`: Leaves the input data unchanged.
- `UpperCase`: Converts all line data to uppercase.
- `LowerCase`: Converts all line data to lowercase.
- `AddMetaData`: Adds metadata (resource name and line number) to the line data.
- `RegexSkipFilter`: Excludes lines that match a given regular expression pattern.
- `RegexKeepFilter`: Includes only the lines that match a given regular expression pattern.
- `RegexSubstituteTransform`: Performs regular expression-based substitutions on per-line data.
- `SkipRepeatedBlankLines`: Skips consecutive blank lines while keeping the first blank line.


Use Cases:
These transformations are intended for use in text processing pipelines, enabling flexible
manipulation of data streams that consist of individual lines of text.

Example Usage:
    ```python
    from pathlib import Path
    from .transforms import UpperCase, RegexSkipFilter

    # Example transformations
    upper_case_transform = UpperCase()
    filter_transform = RegexSkipFilter(pattern=r'^#')  # Skip lines starting with '#'

    # Apply transformations in a pipeline
    input_line = LineStreamItem(sequence_id=1, resource_name="example.txt", data="hello")
    for transformed_line in upper_case_transform.transform(input_line):
        print(transformed_line.data)  # Outputs: "HELLO"
    ```
"""


import re
from typing import Iterable

from ._base import TransformBase
from ._streamitem import LineStreamItem


class PassThrough(TransformBase):
    """A transform that passes lines through unchanged."""

    def transform(self, item: LineStreamItem) -> Iterable[LineStreamItem]:
        yield item


class UpperCase(TransformBase):

    """A transform that converts line data to uppercase."""

    def transform(self, item: LineStreamItem) -> Iterable[LineStreamItem]:
        yield LineStreamItem(sequence_id=item.sequence_id,
                             resource_name=item.resource_name,
                             data=item.data.upper())


class LowerCase(TransformBase):

    """A transform that converts line data to lowercase."""

    def transform(self, item: LineStreamItem) -> Iterable[LineStreamItem]:
        yield LineStreamItem(sequence_id=item.sequence_id,
                             resource_name=item.resource_name,
                             data=item.data.lower())


class AddMetaData(TransformBase):

    """A transform that appends metadata to line data."""

    def transform(self, item: LineStreamItem) -> Iterable[LineStreamItem]:
        new_data = f"{item.resource_name}:{item.sequence_id}:{item.data}"
        yield LineStreamItem(
            sequence_id=item.sequence_id,
            resource_name=item.resource_name,
            data=new_data
        )


class RegexSkipFilter(TransformBase):

    """A transform that skips lines matching a regex pattern."""

    def __init__(self, pattern: str):
        self.regex = re.compile(pattern)

    def transform(self, item: LineStreamItem) -> Iterable[LineStreamItem]:
        if not self.regex.match(item.data):
            yield item


class RegexKeepFilter(TransformBase):

    """A transform that keeps only lines matching a regex pattern."""

    def __init__(self, pattern: str):
        self.regex = re.compile(pattern)

    def transform(self, item: LineStreamItem) -> Iterable[LineStreamItem]:
        if self.regex.match(item.data):
            yield item


class RegexSubstituteTransform(TransformBase):
    """A transform that substitutes content in lines using a regex."""

    def __init__(self, pattern: str, replacement: str):
        self.regex = re.compile(pattern)
        self.replacement = replacement

    def transform(self, item: LineStreamItem) -> Iterable[LineStreamItem]:
        item.data = self.regex.sub(self.replacement, item.data)
        yield item


class SkipRepeatedBlankLines(TransformBase):
    """A transform that skips consecutive blank lines."""

    def __init__(self):
        self.last_was_blank = False

    def transform(self, item: LineStreamItem) -> Iterable[LineStreamItem]:
        is_blank = not item.data.strip()
        if is_blank:
            if not self.last_was_blank:
                self.last_was_blank = True
                yield item
        else:
            self.last_was_blank = False
            yield item
