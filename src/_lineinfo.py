import dataclasses


@dataclasses.dataclass
class LineInfo:
    """
    A data class representing information about a single line of input.

    This class encapsulates metadata about a line, such as the line number,
    the name of the resource it originates from, and the content of the line itself.

    Attributes:
        line_number (int): The positive integer representing the line number
            (must be greater than 0).
        resource_name (str): The name or identifier of the resource the line
            belongs to (must be a non-empty string).
        line (str): The actual content of the line.

    Methods:
        __post_init__(): Automatically validates the attributes after object
            initialization to ensure they meet the specified conditions:
            - `line_number` must be a positive integer (greater than 0).
            - `resource_name` must be a non-empty string.
            - `line` must be a string.

    Raises:
        ValueError: If any of the following conditions are not met:
            - `line_number` is not a positive integer.
            - `resource_name` is not a non-empty string.
            - `line` is not a string.
    """

    line_number: int
    resource_name: str
    line: str

    def __post_init__(self):
        # Verify that `lineno` is a positive integer (> 0)
        if not isinstance(self.line_number, int) or self.line_number <= 0:
            raise ValueError("line_number must be an integer greater than 0")

        # Verify that `resource_name` is a non-empty string
        if not isinstance(self.resource_name, str) or not self.resource_name.strip():
            raise ValueError("resource_name must be a non-empty string")

        # Verify that `line` is a string
        if not isinstance(self.line, str):
            raise ValueError("line must be a string")
