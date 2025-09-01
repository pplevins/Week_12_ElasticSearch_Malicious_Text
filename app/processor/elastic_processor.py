from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from app.loader import DataLoader


class ElasticSearchProcessor:
    def __init__(self):
        self.es = Elasticsearch('http://localhost:9200')
        self._set_mapping()

    def _set_mapping(self):
        mappings = {
            "properties": {
                "TweetID": {
                    "type": "long"
                },
                "CreateDate": {
                    "type": "date"
                },
                "Antisemitic": {
                    "type": "boolean"
                },
                "text": {
                    "type": "text"
                },
                "sentiment": {
                    "type": "keyword"
                },
                "weapons": {
                    "type": "keyword"
                }
            }
        }
        self.es.indices.delete(index='tweets', ignore_unavailable=True)
        self.es.indices.create(index='tweets', ignore=400, body=mappings)

    def _generate_documents(self, tweets):
        for i, doc_data in enumerate(tweets):
            yield {
                "_index": "tweets",
                "_id": i + 1,
                "_source": doc_data
            }

    def _load_to_es(self):
        tweets = DataLoader.load_from_csv()
        success, failed = bulk(self.es, self._generate_documents(tweets))
        print(f"Successfully indexed {success} documents, failed to index {len(failed)} documents.")

    def _update_sentiment(self):
        pass

    def _search_weapons(self):
        pass

    def _delete_unnecessary_documents(self):
        pass

    def process(self):
        self._load_to_es()

    def get_antisemitic_with_weapons(self):
        pass

    def get_with_two_weapons(self):
        pass
