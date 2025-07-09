import re
from typing import Iterable

from _base import TransformBase
from _streamitem import LineStreamItem


class PassThrough(TransformBase):
    def transform(self, lineinfo: LineStreamItem) -> Iterable[LineStreamItem]:
        yield lineinfo

class UpperCase(TransformBase):
    def transform(self, lineinfo: LineStreamItem) -> Iterable[LineStreamItem]:
        yield LineStreamItem(sequence_id=lineinfo.sequence_id,
                             resource_name=lineinfo.resource_name,
                             data=lineinfo.data.upper())


class LowerCase(TransformBase):
    def transform(self, lineinfo: LineStreamItem) -> Iterable[LineStreamItem]:

        yield LineStreamItem(sequence_id=lineinfo.sequence_id,
                             resource_name=lineinfo.resource_name,
                             data=lineinfo.data.lower())


class AddMetaData(TransformBase):
    def transform(self, lineinfo: LineStreamItem) -> Iterable[LineStreamItem]:
        # Create a new LineStreamItem object instead of modifying the original
        new_data = f"{lineinfo.resource_name}:{lineinfo.sequence_id}:{lineinfo.data}"
        yield LineStreamItem(
            sequence_id=lineinfo.sequence_id,
            resource_name=lineinfo.resource_name,
            data=new_data
        )



class RegexSkipFilter(TransformBase):
    def __init__(self, pattern: str):
        """
        Initializes the RegexSkipFilter with a pattern to match lines against.

        Args:
            pattern (str): The regular expression pattern to match lines to skip.
        """
        self.regex = re.compile(pattern)

    def transform(self, lineinfo: LineStreamItem) -> Iterable[LineStreamItem]:
        """
        Filters out lines that match the given regular expression pattern.

        Args:
            lineinfo (LineStreamItem): The line to test against the regex.

        Yields:
            LineStreamItem: Lines that do not match the regex.
        """
        if not self.regex.match(lineinfo.data):
            yield lineinfo

class RegexKeepFilter(TransformBase):
    def __init__(self, pattern: str):
        """
        Initializes the RegexKeepFilter with a pattern to match lines against.

        Args:
            pattern (str): The regular expression pattern to match lines to keep.
        """
        self.regex = re.compile(pattern)

    def transform(self, lineinfo: LineStreamItem) -> Iterable[LineStreamItem]:
        """
        Filters in lines that match the given regular expression pattern.

        Args:
            lineinfo (LineStreamItem): The line to test against the regex.

        Yields:
            LineStreamItem: Lines that do not match the regex.
        """
        if self.regex.match(lineinfo.data):
            yield lineinfo

class RegexSubstituteTransform(TransformBase):
    def __init__(self, pattern: str, replacement: str):
        """
        Initializes the RegexSubstituteTransform with a pattern and replacement.

        Args:
            pattern (str): The regular expression pattern to search for in lines.
            replacement (str): The string to replace the matched content with.
        """
        self.regex = re.compile(pattern)
        self.replacement = replacement

    def transform(self, lineinfo: LineStreamItem) -> Iterable[LineStreamItem]:
        """
        Performs a regex substitution on the line content.

        Args:
            lineinfo (LineStreamItem): The line to process for substitution.

        Yields:
            LineStreamItem: Lines with substituted content (or unchanged if no match).
        """
        lineinfo.data = self.regex.sub(self.replacement, lineinfo.data)
        yield lineinfo


class SkipRepeatedBlankLines(TransformBase):
    def __init__(self):
        """
        Initializes the SkipRepeatedBlankLines transform.

        Keeps track of whether the previously seen line was blank or not.
        """
        self.last_was_blank = False  # State to track if the last line was blank

    def transform(self, lineinfo: LineStreamItem) -> Iterable[LineStreamItem]:
        """
        Transforms the input to skip repeated blank lines.

        Args:
            lineinfo (LineStreamItem): The current line to process.

        Yields:
            LineStreamItem: Lines that are not repeating blank lines.
        """
        is_blank = not lineinfo.data.strip()  # Check if the current line is blank

        if is_blank:
            # Skip if the last line was also blank
            if not self.last_was_blank:
                self.last_was_blank = True
                yield lineinfo
        else:
            # Reset state when encountering a non-blank line
            self.last_was_blank = False
            yield lineinfo
