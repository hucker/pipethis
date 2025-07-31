import dataclasses
import pathlib
from abc import ABC, abstractmethod
from typing import Any, Iterable


@dataclasses.dataclass
class StreamItem(ABC):
    """
    A base class representing streaming data information in a generalized form.

    The sequence ID and resource name can be used for error reporting.  For
    example.

    Pipeline Error: File-data.txt  Line-23  Invalid data - xxx

    Attributes:
        sequence_id (int): A sequential identifier for the data. The meaning is
                           contextual (e.g., line # for text, row # for csv, frame # for video).
                           Some data files don't have an idea of a sequence ID, such
                           as image files.  In this case  just using sequence ID = 1 is fine.
        resource_name (str): Name or identifier of the resource the data belongs to
                             (e.g., file name, sheet name).
        data (Any): The actual data being processed. Subclasses specify the type.
    """

    sequence_id: int  # Line number, page number, frame number
    resource_name: str  # Typically a file name but can be anything useful to end user.
    data: Any  # Data flowing in pipeline

    def __post_init__(self):

        # Avoid confusion and don't allow bad whitespace.
        if isinstance(self.resource_name, str):
            self.resource_name = self.resource_name.strip()

        # Validate `resource_name` (must be a non-empty string)
        if not isinstance(self.resource_name, str) or not self.resource_name:
            raise ValueError("Resource_name must be a non-empty string")

        # Validate `sequence_id` (must be a positive integer)
        if not isinstance(self.sequence_id, int) or self.sequence_id <= 0:
            raise ValueError("Sequence_id must be a positive integer")

        # Abstract validation for subclasses
        self.validate()

    @abstractmethod
    def validate(self):  # pragma no cover
        """Perform additional subclass-specific validation."""
        pass


class FileHandlerBase(ABC):
    """
    Abstract base class for handling files of various types. Supports resource
    management via context manager methods.
    """

    def __init__(self, file_path: pathlib.Path):
        self.file_path = file_path

    @abstractmethod
    def stream(self) -> Iterable[StreamItem]:  # pragma: no cover

        """
        Stream the content of the file as StreamItems.
        """
        pass

    def __enter__(self):  # pragma: no cover

        """
        Prepare resources for file handling.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):  # pragma: no cover

        """
        Clean up resources associated with file handling.
        """
        pass


class InputBase(ABC):  # no cover
    """Base class for all input components."""

    @abstractmethod
    def stream(self) -> Iterable[StreamItem]:  # pragma no cover
        pass


class TransformBase(ABC):  # no cover
    """Base class for all transformation components."""

    @abstractmethod
    def transform(self, item: StreamItem) -> Iterable[StreamItem]:  # pragma no cover
        pass


class OutputBase(ABC):  # pragma no cover
    """Base class for all output components."""

    def __init__(self):
        pass

    def __enter__(self):
        """
        Default context manager behavior.
        Does nothing, as not all outputs need special setup.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Default context manager behavior.
        Does nothing, as not all outputs need special cleanup.
        """
        pass

    @abstractmethod
    def write(self, lineinfo: StreamItem):
        pass

    def close(self):
        pass
