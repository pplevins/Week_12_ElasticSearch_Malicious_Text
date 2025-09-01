import pandas as pd


class DataLoader:
    @staticmethod
    def load_from_csv(path='../../data/tweets_injected.csv'):
        """Loads data from csv file."""
        return pd.read_csv(path).to_dict(orient='records')

    @staticmethod
    def fetch_weapons_list(path='../../data/weapon_list.txt'):
        with open(path, 'r', encoding='utf-8-sig') as file:
            return file.read().splitlines()
