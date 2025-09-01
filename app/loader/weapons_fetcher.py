class WeaponsFetcher:
    @staticmethod
    def fetch_weapons_list(path='../../data/weapon_list.txt'):
        with open(path, 'r', encoding='utf-8-sig') as file:
            return file.read().splitlines()
