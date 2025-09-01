import re


class TextCleaner:
    """A utility class to clean up text and preprocess it."""

    def lower_text(self, text):
        """Lowers text."""
        return text.lower()

    def clean_punctuation(self, text):
        """Cleans all punctuation and special characters."""
        # TODO: can also be done with: re.sub(r'[^\w\s]+', '', text)
        # return text.translate(str.maketrans('', '', string.punctuation))
        return re.sub(r'[^\w\s]+', '', text)

    def remove_duplicate_whitespaces(self, text):
        """Removes duplicate whitespaces."""
        return " ".join(re.split(r"\s+", text, flags=re.UNICODE)).strip()
