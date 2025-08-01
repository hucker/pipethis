from PIL import Image, ImageEnhance

from ._base import TransformBase
from ._streamitem import ImageStreamItem


class ImageEnhancerTransformer(TransformBase):
    """
    A customizable transformation class for image enhancement using parameters such as
    brightness, contrast, saturation, sharpness, and image mode.
    """

    VALID_MODES = {"", "1", "L", "P", "RGB", "RGBA", "CMYK", "YCbCr", "LAB", "HSV", "I", "F"}

    def __init__(self, transform_string: str = None,  # Change default to `None`
                 brightness: float = 1.0,
                 contrast: float = 1.0,
                 saturation: float = 1.0,
                 sharpness: float = 1.0):
        """
        Initializes the ImageEnhancerTransformer with specified transformation parameters.
        """
        if transform_string and not self._validate_mode(transform_string):
            raise ValueError(f"Invalid mode '{transform_string}'. Available modes: {', '.join(self.VALID_MODES)}.")

        self.transform_string = transform_string  # Can be `None` now
        self.brightness = brightness
        self.contrast = contrast
        self.saturation = saturation
        self.sharpness = sharpness

    def _validate_mode(self, mode: str):
        return mode in self.VALID_MODES

    def transform(self, stream_item: ImageStreamItem) -> ImageStreamItem:
        """
        Applies the configured image enhancements and mode transformation to an ImageStreamItem.
        """
        if not isinstance(stream_item, ImageStreamItem):
            raise TypeError("Expected an ImageStreamItem")

        transformed_image: Image.Image = stream_item.data

        # Apply mode conversion only if `xform_str` is explicitly set
        if self.transform_string:
            transformed_image = transformed_image.convert(self.transform_string)

        # Apply brightness adjustment if configured
        if self.brightness != 1.0:
            transformed_image = ImageEnhance.Brightness(transformed_image).enhance(
                self.brightness
            )

        # Apply contrast adjustment if configured
        if self.contrast != 1.0:
            transformed_image = ImageEnhance.Contrast(transformed_image).enhance(
                self.contrast
            )

        # Apply saturation adjustment if configured
        if self.saturation != 1.0:
            transformed_image = ImageEnhance.Color(transformed_image).enhance(
                self.saturation
            )

        # Apply sharpness adjustment if configured
        if self.sharpness != 1.0:
            transformed_image = ImageEnhance.Sharpness(transformed_image).enhance(
                self.sharpness
            )

        # Return a new ImageStreamItem with the transformed image
        return ImageStreamItem(
            sequence_id=stream_item.sequence_id,
            resource_name=stream_item.resource_name,
            data=transformed_image
        )
