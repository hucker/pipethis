import pytest
from PIL import Image, ImageEnhance
# noinspection PyProtectedMember
from pipethis._image_transform import ImageEnhancerTransformer, ImageStreamItem


# ---------------------------
# Test Fixtures
# ---------------------------

@pytest.fixture
def solid_color_image():
    """Fixture for a simple solid color image (blue)."""
    return Image.new("RGB", (4, 4), color=(50, 100, 150))  # Small and predictable


@pytest.fixture
def grayscale_image():
    """Fixture for a grayscale image."""
    return Image.new("L", (4, 4), color=128)  # Mid-tone gray


@pytest.fixture
def colorful_gradient_image():
    """Fixture for a colorful gradient image."""
    image = Image.new("RGB", (4, 4))
    pixels = image.load()
    for x in range(4):
        for y in range(4):
            pixels[x, y] = (x * 64, y * 64, (x + y) * 32)  # Gradient pattern
    return image


@pytest.fixture
def stream_item_solid(solid_color_image):
    """Fixture for an ImageStreamItem with a solid blue image."""
    return ImageStreamItem(sequence_id=1, resource_name="solid_blue.jpg", data=solid_color_image)


@pytest.fixture
def stream_item_grayscale(grayscale_image):
    """Fixture for an ImageStreamItem with a grayscale image."""
    return ImageStreamItem(sequence_id=2, resource_name="grayscale.jpg", data=grayscale_image)


@pytest.fixture
def stream_item_gradient(colorful_gradient_image):
    """Fixture for an ImageStreamItem with a gradient image."""
    return ImageStreamItem(sequence_id=3, resource_name="gradient.jpg", data=colorful_gradient_image)


# ---------------------------
# Helper Function for Precision Test
# ---------------------------

def calculate_brightness(color, brightness_factor):
    """Mathematically calculate brightness-adjusted RGB values."""
    return tuple(min(255, max(0, int(c * brightness_factor))) for c in color)


def calculate_contrast(color, contrast_factor):
    """
    Recreate PIL's ImageEnhance.Contrast logic and apply it to a single pixel value.
    """
    # Create a dummy 1x1 image with the provided color
    dummy_image = Image.new("RGB", (1, 1), color=color)
    enhancer = ImageEnhance.Contrast(dummy_image)

    # Apply the contrast adjustment
    adjusted_image = enhancer.enhance(contrast_factor)

    # Return the adjusted pixel as a tuple
    return adjusted_image.getpixel((0, 0))



# ---------------------------
# Actual Test Cases
# ---------------------------

# Test Mode Conversion
def test_mode_conversion(stream_item_solid):
    transformer = ImageEnhancerTransformer(xform_str="L")
    transformed_item = transformer.transform(stream_item_solid)

    assert transformed_item.data.mode == "L"  # Ensure mode is grayscale
    assert transformed_item.data.size == stream_item_solid.data.size


# Test Brightness Adjustment with Mathematical Precision
def test_brightness_calculation(stream_item_solid):
    # Given: A transformer with brightness adjustment
    transformer = ImageEnhancerTransformer(brightness=1.5)  # Brightness factor 1.5

    # When: Transform is applied
    transformed_item = transformer.transform(stream_item_solid)

    # Assert: Transformed image remains in RGB mode
    assert transformed_item.data.mode == "RGB", f"Expected RGB mode, got {transformed_item.data.mode}"

    # Validate every pixel matches the mathematical transformation: R, G, B = R*1.5, G*1.5, B*1.5
    original_pixels = list(stream_item_solid.data.getdata())  # Original pixels from fixture
    transformed_pixels = list(transformed_item.data.getdata())  # Pixels after transformation

    for original, transformed in zip(original_pixels, transformed_pixels):
        assert transformed == calculate_brightness(original, 1.5)


# Test Contrast Adjustment with Mathematical Precision
def test_contrast_calculation(stream_item_solid):
    transformer = ImageEnhancerTransformer(contrast=1.2)  # Contrast factor 1.2
    transformed_item = transformer.transform(stream_item_solid)

    # Validate every pixel matches the expected adjustment
    original_pixels = list(stream_item_solid.data.getdata())
    transformed_pixels = list(transformed_item.data.getdata())

    for original, transformed in zip(original_pixels, transformed_pixels):
        assert transformed == calculate_contrast(original, 1.2)


# Test Saturation Adjustment (Color Enhancer)
def test_saturation_calculation(stream_item_gradient):
    transformer = ImageEnhancerTransformer(saturation=2.0)  # Double saturation
    transformed_item = transformer.transform(stream_item_gradient)

    # Validate the transformed image mode remains RGB (no mode change)
    assert transformed_item.data.mode == "RGB"

    # Placeholder: Saturation can be manually verified visually or via precomputed ranges
    assert transformed_item.data.size == stream_item_gradient.data.size  # Size is unchanged


# Test Sharpness Adjustment for Larger Patterns
def test_sharpness_adjustment(stream_item_grayscale):
    transformer = ImageEnhancerTransformer(sharpness=1.5)  # Slight sharpness increase
    transformed_item = transformer.transform(stream_item_grayscale)

    # Can't directly calculate sharpness modification. Ensure successful transformation
    assert transformed_item.data.size == stream_item_grayscale.data.size
    assert transformed_item.data.mode == "L"


# Combined Transformations Test
def test_combined_transformations(stream_item_gradient):
    # Apply multiple enhancements together
    transformer = ImageEnhancerTransformer(
        xform_str="L",  # Convert to grayscale
        brightness=1.2,  # Increase brightness
        contrast=1.5,  # Increase contrast
        saturation=1.0,  # Keep saturation (ignored for grayscale)
        sharpness=1.1  # Slight sharpness enhancement
    )
    transformed_item = transformer.transform(stream_item_gradient)

    assert transformed_item.data.mode == "L"  # Check that the image is grayscale
    assert transformed_item.data.size == stream_item_gradient.data.size  # Unchanged size

def test_invalid_transform_string():
    """Test that an invalid xform_str raises a ValueError."""
    with pytest.raises(ValueError, match="Invalid mode 'INVALID_MODE'.*"):
        ImageEnhancerTransformer(xform_str="INVALID_MODE")  # Invalid mode

def test_transform_invalid_input_type():
    """Test that passing a non-ImageStreamItem object raises a TypeError."""
    transformer = ImageEnhancerTransformer()  # Instantiate the transformer
    invalid_input = "not_an_image_stream_item"  # Invalid input (not an ImageStreamItem instance)

    # Assert that transform() raises a TypeError with the expected message
    with pytest.raises(TypeError, match="Expected an ImageStreamItem"):
        transformer.transform(invalid_input)
