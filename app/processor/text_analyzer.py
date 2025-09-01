import os

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

from loader import DataLoader
from .text_cleaner import TextCleaner


class TextAnalyzer:
    def __init__(self):
        nltk_dir = "/tmp/nltk_data"
        os.makedirs(nltk_dir, exist_ok=True)
        nltk.data.path.append(nltk_dir)
        nltk.download('vader_lexicon', download_dir=nltk_dir, quiet=True)  # Compute sentiment labels
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        self.weapons_list = DataLoader.fetch_weapons_list()
        self.text_cleaner = TextCleaner()

    def calculate_text_sentiment(self, tweet_text: str) -> str:
        compound = self.sentiment_analyzer.polarity_scores(tweet_text).get('compound')
        return "positive" if compound > 0.5 \
            else "negative" if compound < -0.5 else "neutral"

    def find_weapons(self, tweet_text: str) -> str | None:
        weapons = []
        for weapon in self.weapons_list:
            weapon = self.text_cleaner.remove_duplicate_whitespaces(
                self.text_cleaner.lower_text(
                    self.text_cleaner.clean_punctuation(weapon)
                )
            )
            if f" {weapon} " in tweet_text:
                weapons.append(weapon)
        return weapons if len(weapons) > 0 else None
