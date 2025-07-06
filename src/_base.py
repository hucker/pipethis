from abc import ABC, abstractmethod
from typing import Iterable

from _lineinfo import LineInfo


class InputBase(ABC):
    """Base class for all input components."""

    @abstractmethod
    def stream(self) -> Iterable[LineInfo]:
        pass # pragma: no cover



class TransformBase(ABC):
    """Base class for all transformation components."""

    @abstractmethod
    def transform(self, lineinfo: LineInfo) -> Iterable[LineInfo]:
        pass # pragma: no cover



class OutputBase(ABC):
    """Base class for all output components."""
    def __init__(self):

        # Track Bytes Written
        self._size = 0

    def update_size(self,text):
        self._size += len(text)

    @property
    def size(self):
        # Number of bytes written.
        return self._size

    @abstractmethod
    def write(self, lineinfo: LineInfo):
        pass # pragma: no cover

    def close(self):
        pass # pragma: no cover

