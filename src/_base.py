from abc import ABC, abstractmethod
from typing import Iterable,Any
import dataclasses


@dataclasses.dataclass
class DataInfo(ABC):
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

    sequence_id: int     # Line number, page number, frame number
    resource_name: str   # Typically a file name but can be anything useful to end user.
    data: Any            # Data flowing in pipeline

    def __post_init__(self):

        # Avoid confusion and don't allow bad whitespace.
        if isinstance(self.resource_name,str):
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
    def validate(self):
        """Perform additional subclass-specific validation."""
        pass # pragma: no cover


class InputBase(ABC):
    """Base class for all input components."""

    @abstractmethod
    def stream(self) -> Iterable[DataInfo]:
        pass # pragma: no cover



class TransformBase(ABC):
    """Base class for all transformation components."""

    @abstractmethod
    def transform(self, lineinfo: DataInfo) -> Iterable[DataInfo]:
        pass # pragma: no cover



class OutputBase(ABC):
    """Base class for all output components."""
    def __init__(self):

        # Track Bytes Written
        self._size = 0

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


    def update_size(self,text):
        self._size += len(text)

    @property
    def size(self):
        # Number of bytes written.
        return self._size

    @abstractmethod
    def write(self, lineinfo: DataInfo):
        pass # pragma: no cover

    def close(self):
        pass # pragma: no cover

