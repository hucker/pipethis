import io
import logging

import pytest

from pipethis._input_from_file import FromFile
from pipethis._input_from_folder import FromFolder
from pipethis._input_from_glob import FromGlob
from pipethis._logging import DEFAULT_NAME, get_logger, initialize_pipethis_logger, pipethis_setup_logging


@pytest.fixture(autouse=True)
def reset_logger_state():
    # Automatically reset the logger state before every test
    initialize_pipethis_logger()


@pytest.mark.parametrize(
    "log_level,expected_messages,unexpected_messages",
    [
        (
                logging.DEBUG,
                ["This is a debug-level log.", "This is an info-level log.", "This is an error-level log."],
                [],
        ),
        (
                logging.INFO,
                ["This is an info-level log.", "This is an error-level log."],
                ["This is a debug-level log."],
        ),
        (
                logging.WARNING,
                ["This is an error-level log."],
                ["This is a debug-level log.", "This is an info-level log."],
        ),
    ],
)
def test_logging_levels_with_setup(caplog, log_level, expected_messages, unexpected_messages):
    # Set up the logger with the parameterized log level
    logger = pipethis_setup_logging(level=log_level)

    # Generate log messages
    logger.debug("This is a debug-level log.")
    logger.info("This is an info-level log.")
    logger.error("This is an error-level log.")

    # Verify expected messages are present
    for message in expected_messages:
        assert message in caplog.text, f"Expected message '{message}' not found at log level {log_level}"

    # Verify unexpected messages are not present
    for message in unexpected_messages:
        assert message not in caplog.text, f"Unexpected message '{message}' found at log level {log_level}"


def test_pipethis_logger_name():
    # Step 1: Create an in-memory stream to capture logs
    stream = io.StringIO()

    # Step 2: Ensure the logger is in its default state (NullHandler only)
    initialize_pipethis_logger()

    # Step 3: Configure the logger with a specific name and a stream handler
    logger_name = "test_logger"
    pipethis_logger = pipethis_setup_logging(
        level=logging.DEBUG,
        name=logger_name,
        stream_=stream,
        format_string='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Step 4: Log a message from the child logger
    pipethis_logger.info("This is a test message")

    # Step 5: Retrieve logged output
    logged_output = stream.getvalue()

    # Step 6: Verify the logger name and log content
    assert f"{DEFAULT_NAME}.{logger_name}" in logged_output, f"Logger name '{DEFAULT_NAME}.{logger_name}' not found in log output"
    assert "This is a test message" in logged_output, "Log message not found in log output"
    assert "INFO" in logged_output, "Log level not found in log output"


def test_logger_file_output(tmp_path):
    # Step 1: Create a temporary log file path
    log_file = tmp_path / "test_log.log"

    # Step 2: Set up the logger to write to the file
    pipethis_logger = pipethis_setup_logging(level=logging.DEBUG, file_name=log_file)

    # Step 3: Log some test messages
    pipethis_logger.debug("Debug message")
    pipethis_logger.info("Info message")
    pipethis_logger.warning("Warning message")
    pipethis_logger.error("Error message")

    # Step 4: Read the log file to verify the contents
    with open(log_file, "r") as file:
        log_content = file.read()

    # Step 5: Verify that log messages are present in the file
    assert "Debug message" in log_content
    assert "Info message" in log_content
    assert "Warning message" in log_content
    assert "Error message" in log_content

    # Step 6: Optionally, verify log levels and the presence of timestamps
    assert "DEBUG" in log_content
    assert "INFO" in log_content
    assert "WARNING" in log_content
    assert "ERROR" in log_content
    assert " - pipethis - " in log_content  # Verifying the logger name


def test_pipethis_logger_with_stream():
    # Step 1: Create an in-memory stream for logging
    stream = io.StringIO()

    # Step 2: Configure the logger with the stream
    pipethis_logger = pipethis_setup_logging(level=logging.DEBUG, stream_=stream)

    # Step 3: Log some messages
    pipethis_logger.info("Test info message")
    pipethis_logger.warning("Test warning message")

    # Step 4: Retrieve the stream content
    log_content = stream.getvalue()

    # Step 5: Assert the messages are properly logged in the stream
    assert "Test info message" in log_content
    assert "Test warning message" in log_content
    assert "INFO" in log_content
    assert "WARNING" in log_content


def test_file_handler_val_error():
    # Use an invalid directory to trigger OSError (non-existent or invalid path)
    invalid_file_name = "/invalid_path/test_log.log"

    # Verify ValueError is raised when OSError is encountered
    with pytest.raises(ValueError):
        pipethis_setup_logging(file_name=invalid_file_name)


def test_invalid_stream_missing_write():
    # Step 1: Create an invalid stream (object without 'write' attribute)
    invalid_stream = object()

    # Step 2: Pass the invalid stream to pipethis_setup_logging and expect a ValueError
    with pytest.raises(ValueError):  # , match="is not a valid writable stream"):
        pipethis_setup_logging(stream_=invalid_stream)


def test_fromfile_logs_debug_message(tmp_path):
    """
    Test that the FromFile class logs the correct debug message when initialized.
    """
    # Create a temporary log file
    log_file = tmp_path / "debug_test.log"

    # Set up logging using pipethis_setup_logging and direct it to the log file
    logger = pipethis_setup_logging(
        level=logging.DEBUG,  # Set level to DEBUG
        file_name=str(log_file),  # Output logs to the temporary file
        format_string="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create a temporary test file for FromFile
    test_file = tmp_path / "test_file.log"
    test_file.touch()  # Create an empty file

    # Create an instance of FromFile (this should log a debug message)
    FromFile(filepath=test_file)

    # Read the log file to check if the expected log message is present
    with open(log_file, "r") as log:
        log_contents = log.read()

    # Assert that there is a DEBUG log entry in the log file
    assert "DEBUG" in log_contents, "Expected DEBUG log message, but none was found."

    # Assert that the expected initialization debug message appears in the log file
    expected_log_message = f"Init FromFile with path: {test_file.resolve()}"
    assert expected_log_message in log_contents, f"Expected log message: '{expected_log_message}', but it was not found."


def test_fromfolder_logs_debug_message(tmp_path):
    """
    Test that the FromFolder class logs the correct debug message when initialized.
    """
    # Create a temporary log file
    log_file = tmp_path / "debug_test.log"

    # Set up logging using pipethis_setup_logging and direct it to the log file
    logger = pipethis_setup_logging(
        level=logging.DEBUG,  # Set level to DEBUG
        file_name=str(log_file),  # Output logs to the temporary file
        format_string="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create an instance of FromFolder (this should log a debug message)
    FromFolder(folder_path=tmp_path)

    # Read the log file to check if the expected log message is present
    with open(log_file, "r") as log:
        log_contents = log.read()

    # Assert that there is a DEBUG log entry in the log file
    assert "DEBUG" in log_contents, "Expected DEBUG log message, but none was found."

    # Assert that the expected initialization debug message appears in the log file
    expected_log_message = f"Init FromFolder with path:"
    assert expected_log_message in log_contents, f"Expected log message: '{expected_log_message}', but it was not found."


def test_fromglob_logs_debug_message(tmp_path):
    """
    Test that the FromGlob class logs the correct debug message when initialized.
    """
    # Create a temporary log file
    log_file = tmp_path / "debug_test.log"

    # Set up logging using pipethis_setup_logging and direct it to the log file
    logger = pipethis_setup_logging(
        level=logging.DEBUG,  # Set level to DEBUG
        file_name=str(log_file),  # Output logs to the temporary file
        format_string="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create an instance of FromFolder (this should log a debug message)
    FromGlob(folder_path=tmp_path)

    # Read the log file to check if the expected log message is present
    with open(log_file, "r") as log:
        log_contents = log.read()

    # Assert that there is a DEBUG log entry in the log file
    assert "DEBUG" in log_contents, "Expected DEBUG log message, but none was found."


def test_fromglob_no_debug_logs_when_level_is_warning(tmp_path):
    """
    Test that FromGlob does not log debug messages when the log level is set to WARNING.

    Since the initialization logs are at DEBUG level, setting the logger to WARNING
    should prevent any messages from appearing in the log file.
    """
    # Create a temporary log file
    log_file = tmp_path / "warning_test.log"

    # Set up logging using pipethis_setup_logging with WARNING level
    logger = pipethis_setup_logging(
        level=logging.WARNING,  # Set level to WARNING
        file_name=str(log_file),  # Output logs to the temporary file
        format_string="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create an instance of FromGlob (this should not log a debug message due to WARNING level)
    FromGlob(folder_path=tmp_path)

    # Read the log file to check if it's empty
    with open(log_file, "r") as log:
        log_contents = log.read()

    # Assert that there is no DEBUG log entry in the log file
    assert "DEBUG" not in log_contents, "Unexpected DEBUG log message found when level is WARNING"

    # Assert that the initialization message is not present
    assert "Init FromGlob with path" not in log_contents, "Unexpected initialization message found when level is WARNING"

    # The log file should be empty or contain only header/metadata (if any)
    assert len(log_contents.strip()) == 0, "Log file should be empty when level is WARNING"


@pytest.mark.parametrize(
    "invalid_name",
    [
        "invalid_name",
        "",  # Empty string
        "my.custom.logger",  # Uses periods but doesn't start with DEFAULT_NAME
        "logging",  # A valid Python logger name but not starting with DEFAULT_NAME
        123,  # Non-string input
    ]
)
def test_get_logger_with_invalid_name(invalid_name):
    """
    Test that get_logger raises a ValueError when given an invalid name
    that doesn't start with DEFAULT_NAME.
    """
    # Setup
    # Convert to string for non-string inputs in the error message

    # Execute & Assert
    with pytest.raises(ValueError) as exc_info:
        get_logger(invalid_name)



