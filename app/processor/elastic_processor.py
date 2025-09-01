from elasticsearch import Elasticsearch


class ElasticSearchProcessor:
    def __init__(self):
        self.es = Elasticsearch('localhost:9200')

    def _set_mapping(self):
        pass

    def _load_to_es(self):
        pass

    def _update_sentiment(self):
        pass

    def _search_weapons(self):
        pass

    def _delete_unnecessary_documents(self):
        pass

    def process(self):
        pass

    def get_antisemitic_with_weapons(self):
        pass

    def get_with_two_weapons(self):
        pass
