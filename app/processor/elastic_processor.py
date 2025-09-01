import re
from time import sleep

from dateutil import parser
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from loader import DataLoader
from processor.text_analyzer import TextAnalyzer


class ElasticSearchProcessor:
    def __init__(self):
        self.es = Elasticsearch('http://elastic:9200')
        self._set_mapping()
        self._text_analyzer = TextAnalyzer()

    def _set_mapping(self):
        mappings = {
            "properties": {
                "TweetID": {
                    "type": "long"
                },
                "CreateDate": {
                    "type": "date",
                    "format": "yyyy-MM-dd'T'HH:mm:ssXXX"
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
        self.es.options(ignore_status=[400]).indices.create(
            index="tweets",
            mappings=mappings
        )

    def _generate_documents(self, tweets):
        for i, doc_data in enumerate(tweets):
            yield {
                "_index": "tweets",
                "_id": i + 1,
                "_source": {
                    'TweetID': int(doc_data['TweetID']),
                    'CreateDate': parser.parse(doc_data['CreateDate']).isoformat(),
                    'Antisemitic': bool(doc_data['Antisemitic']),
                    'text': doc_data['text']
                }
            }

    def _load_to_es(self):
        tweets = DataLoader.load_from_csv()
        success, failed = bulk(self.es, self._generate_documents(tweets))
        print(f"Successfully indexed {success} documents, failed to index {len(failed)} documents.")
        sleep(5)

    def _update_sentiment(self):
        scroll = self.es.search(
            index="tweets",
            scroll="2m",
            body={
                "_source": ["text"],  # only fetch Tweet field to save bandwidth
                "query": {"match_all": {}},
                "size": 10000,  # batch size
            }
        )

        scroll_id = scroll["_scroll_id"]
        hits = scroll["hits"]["hits"]

        while hits:
            actions = []
            for hit in hits:
                doc_id = hit["_id"]
                tweet_text = hit["_source"].get("text", "")
                sentiment = self._text_analyzer.calculate_text_sentiment(tweet_text)

                actions.append({
                    "_op_type": "update",
                    "_index": "tweets",
                    "_id": doc_id,
                    "doc": {"sentiment": sentiment}
                })

            if actions:
                success = bulk(self.es, actions)
                print(f"Updated {success} docs")

            # Get next batch
            scroll = self.es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = scroll["_scroll_id"]
            hits = scroll["hits"]["hits"]
        # sleep(5)

    def _search_weapons(self):
        weapons_list = DataLoader.fetch_weapons_list()
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"match_phrase": {"text": f" {weapon.lower().strip()}"}} for weapon in
                        weapons_list
                    ]
                }
            },
            "size": 10000,
            "highlight": {
                "fields": {
                    "text": {}
                }
            }
        }

        scroll = self.es.search(index="tweets", body=query, scroll="2m")
        scroll_id = scroll["_scroll_id"]
        hits = scroll["hits"]["hits"]
        print(f"Found {len(hits)} weapons")

        while hits:
            actions = []
            for hit in hits:
                doc_id = hit["_id"]
                highlight = hit["highlight"]["text"]
                found_weapons = []
                for high in highlight:
                    found_weapons += re.findall(r'<em>(.*?)</em>', high)
                if found_weapons:
                    actions.append({
                        "_op_type": "update",
                        "_index": "tweets",
                        "_id": doc_id,
                        "doc": {"weapons": found_weapons}
                    })

            if actions:
                success, failed = bulk(self.es, actions)
                print(f"Updated weapons field in {success} docs, failed to index {len(failed)} documents.")

            # Get next batch
            scroll = self.es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = scroll["_scroll_id"]
            hits = scroll["hits"]["hits"]
        sleep(5)

    def _delete_unnecessary_documents(self):
        response = self.es.delete_by_query(
            index="tweets",
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"Antisemitic": False}},
                            {"terms": {"sentiment": ["neutral", "positive"]}},
                            {"bool": {"must_not": {"exists": {"field": "weapons"}}}}
                        ]
                    }
                }
            }
        )

        print(f'deleted {response['deleted']} documents')

    def process(self):
        self._load_to_es()
        self._update_sentiment()
        self._search_weapons()
        self._delete_unnecessary_documents()

    def get_antisemitic_with_weapons(self):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"Antisemitic": True}},
                        {"exists": {"field": "weapons"}}
                    ]
                }
            },
            "size": 10000
        }
        res = self.es.search(index="tweets", body=query)
        return [hit["_source"] for hit in res["hits"]["hits"]]

    def get_with_two_weapons(self):
        query = {
            "query": {
                "script": {
                    "script": "doc['weapons'].size() >= 2"
                }
            },
            "size": 10000
        }
        res = self.es.search(index="tweets", body=query)
        return [hit["_source"] for hit in res["hits"]["hits"]]


if __name__ == '__main__':
    ElasticSearchProcessor().process()
