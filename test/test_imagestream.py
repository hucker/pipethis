import pytest
from PIL import Image
from pipethis._streamitem import ImageStreamItem

def test_image_stream_item_validate_valid_data():
    """Test validate method with valid PIL.Image.Image data."""
    # Create a valid PIL image
    valid_image = Image.new("RGB", (10, 10))  # Simple blank 10x10 RGB image
    valid_item = ImageStreamItem(sequence_id=1, resource_name="test_image.jpg", data=valid_image)

    # The validate method should not raise any exception
    assert valid_item.validate()


def test_image_stream_item_validate_invalid_data():
    """Test validate method with invalid (non-image) data."""

    # Expect a ValueError with a specific message
    with pytest.raises(ValueError):
        _ = ImageStreamItem(sequence_id=2, resource_name="invalid_image", data="not_an_image")
