import re
from typing import Iterable

from _base import TransformBase
from _lineinfo import LineInfo


class PassThrough(TransformBase):
    def transform(self, lineinfo: LineInfo) -> Iterable[LineInfo]:
        yield lineinfo

class UpperCase(TransformBase):
    def transform(self, lineinfo: LineInfo) -> Iterable[LineInfo]:
        lineinfo.data = lineinfo.data.upper()
        yield lineinfo


class LowerCase(TransformBase):
    def transform(self, lineinfo: LineInfo) -> Iterable[LineInfo]:
        lineinfo.data = lineinfo.data.lower()
        yield lineinfo


class AddMetaData(TransformBase):
    def transform(self, lineinfo: LineInfo) -> Iterable[LineInfo]:
        lineinfo.data = f"{lineinfo.resource_name}:{lineinfo.sequence_id}:{lineinfo.data}"
        yield lineinfo


class RegexSkipFilter(TransformBase):
    def __init__(self, pattern: str):
        """
        Initializes the RegexSkipFilter with a pattern to match lines against.

        Args:
            pattern (str): The regular expression pattern to match lines to skip.
        """
        self.regex = re.compile(pattern)

    def transform(self, lineinfo: LineInfo) -> Iterable[LineInfo]:
        """
        Filters out lines that match the given regular expression pattern.

        Args:
            lineinfo (LineInfo): The line to test against the regex.

        Yields:
            LineInfo: Lines that do not match the regex.
        """
        if not self.regex.match(lineinfo.data):
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

    def transform(self, lineinfo: LineInfo) -> Iterable[LineInfo]:
        """
        Performs a regex substitution on the line content.

        Args:
            lineinfo (LineInfo): The line to process for substitution.

        Yields:
            LineInfo: Lines with substituted content (or unchanged if no match).
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

    def transform(self, lineinfo: LineInfo) -> Iterable[LineInfo]:
        """
        Transforms the input to skip repeated blank lines.

        Args:
            lineinfo (LineInfo): The current line to process.

        Yields:
            LineInfo: Lines that are not repeating blank lines.
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
