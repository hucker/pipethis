import pytest
import json
import datetime as dt
from pipethis._output_to_json import ToJson
from pipethis._input_from_string import FromString

def test_to_json_with_string_input(tmp_path):
    file_name = tmp_path / "test.json"

    # Paranoia on my part that this is a was not correctly deleted
    assert file_name.exists() == False

    string_input = "This is a string input\nwith multiple lines"
    pipeline = FromString(string_input,sep='\n',name='text_input') | ToJson(file_name=file_name)
    result = pipeline.run()

    json_data = json.loads(file_name.read_text())
    assert json_data['header']['count'] == 2
    assert json_data['records'][0]['data'] == "This is a string input"
    assert json_data['records'][0]['sequence_id'] == 1
    assert json_data['records'][0]['resource_name'] == "text_input"

    assert json_data['records'][1]['data'] == "with multiple lines"
    assert json_data['records'][1]['resource_name'] == "text_input"
    assert json_data['records'][1]['sequence_id'] == 2

def test_to_json_with_run_date(tmp_path):
    file_name = tmp_path / "test_with_date.json"

    # Define a specific run_date for testing
    run_date = dt.datetime(2023, 11, 5, 15, 30, 45)  # Use a fixed datetime

    string_input = "Testing run_date parameter"
    pipeline = FromString(string_input, sep='\n', name='date_test') | ToJson(file_name=file_name, run_date=run_date)

    # Run the pipeline
    pipeline.run()

    # Read and parse the output JSON
    json_data = json.loads(file_name.read_text())

    # Verify the run_date in the header
    assert json_data['header']['date'] == run_date.isoformat()
    # Verify the other attributes for consistency
    assert json_data['header']['count'] == 1
    assert json_data['records'][0]['data'] == "Testing run_date parameter"
    assert json_data['records'][0]['resource_name'] == "date_test"
    assert json_data['records'][0]['sequence_id'] == 1

def test_to_json_with_no_data(tmp_path):
    file_name = tmp_path / "empty.json"

    # Create ToJson with no records
    pipeline = ToJson(file_name=file_name)

    # Run pipeline (no data added)
    pipeline.close()

    # Read and parse the output JSON
    json_data = json.loads(file_name.read_text())

    # Verify header and records
    assert json_data['header']['description'] == "JSON Data"
    assert json_data['header']['count'] == 0
    assert json_data['records'] == []

def test_to_json_with_custom_description(tmp_path):
    file_name = tmp_path / "custom_description.json"

    # Custom description
    description = "Custom JSON Description"
    pipeline = ToJson(file_name=file_name, description=description)

    # Run pipeline (empty records)
    pipeline.close()

    # Read and parse the output JSON
    json_data = json.loads(file_name.read_text())

    # Verify the description in the header
    assert json_data['header']['description'] == description

def test_to_json_invalid_date_type(tmp_path):
    file_name = tmp_path / "invalid_date.json"

    # Pass an invalid run_date type (e.g., an integer)
    with pytest.raises(ValueError, match=r"Invalid run_date type.*"):
        pipeline = ToJson(file_name=file_name, run_date=12345)

def test_to_json_with_non_ascii(tmp_path):
    file_name = tmp_path / "non_ascii.json"

    # Data with non-ASCII characters
    string_input = "PipèThîs Tèst"
    pipeline = FromString(string_input, sep='\n', name='non_ascii_test') | ToJson(file_name=file_name, encoding="utf-8")

    # Run pipeline
    pipeline.run()

    # Read file content
    json_data = json.loads(file_name.read_text(encoding="utf-8"))

    # Verify data presence
    assert json_data['records'][0]['data'] == "PipèThîs Tèst"
    assert json_data['header']['count'] == 1

def test_to_json_with_string_run_date(tmp_path):
    file_name = tmp_path / "string_run_date.json"

    # Define a specific date string
    run_date = "2023-11-05T15:30:45"  # ISO 8601 formatted date

    string_input = "Date as string test"
    pipeline = FromString(string_input, sep='\n', name='date_string_test') | ToJson(file_name=file_name,
                                                                                    run_date=run_date)

    # Run the pipeline
    pipeline.run()

    # Read and parse the output JSON
    json_data = json.loads(file_name.read_text())

    # Verify the run_date in the header matches the input string
    assert json_data['header']['date'] == run_date
    # Verify the other fields
    assert json_data['header']['count'] == 1
    assert json_data['records'][0]['data'] == "Date as string test"
    assert json_data['records'][0]['resource_name'] == "date_string_test"
    assert json_data['records'][0]['sequence_id'] == 1
