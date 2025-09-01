import pandas as pd


class DataLoader:
    @staticmethod
    def load_data(path='../../data/tweets_injected.csv'):
        """Loads data from csv file."""
        return pd.read_csv(path).to_dict(orient='records')
