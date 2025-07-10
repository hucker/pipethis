from _streamitem import ImageStreamItem



class GrayscaleTransformer:
    """
    Transforms an ImageStreamItem into a grayscale image.
    """

    def transform(self, stream_item):
        """
        Convert the image in the stream item into grayscale.

        :param stream_item: ImageStreamItem to transform.
        :return: Transformed ImageStreamItem.
        """
        if not isinstance(stream_item, ImageStreamItem):
            raise TypeError("Expected an ImageStreamItem")

        # Convert the image to grayscale
        grayscale_image = stream_item.data.convert("L")

        # Return a new ImageStreamItem with the grayscale image
        return ImageStreamItem(
            sequence_id=stream_item.sequence_id,
            resource_name=stream_item.resource_name,
            data=grayscale_image
        )
