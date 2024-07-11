from elasticsearch7 import Elasticsearch
from elasticsearch7.helpers import bulk
from confluent_kafka import Consumer
from dotenv import load_dotenv
import os
import time
import json


class Logging():
    def __init__(self):
        load_dotenv()
        self.kafka_server = os.getenv("KAFKA_SERVER", "")
        self.kafka_groupid = os.getenv("KAFKA_GROUPID", "")
        self.kafka_offset = os.getenv("KAFKA_OFFSET", "")
        self.kafka_topic = os.getenv("KAFKA_TOPIC", "")

        self.es_host = os.getenv("ES_HOST", "")
        self.es_user = os.getenv("ES_USER", "")
        self.es_pass = os.getenv("ES_PASS", "")
        self.es_index_prefix = os.getenv("ES_INDEX_PREFIX", "")

    def connect_kafka(self):
        consumer = Consumer({
            'bootstrap.servers': self.kafka_server,
            'group.id': self.kafka_groupid,
            'auto.offset.reset': self.kafka_offset
            }
        )
        consumer.subscribe([self.kafka_topic])
        return consumer

    def connect_elasticsearch(self):
        if ':' in self.es_host:
            host, port = self.es_host.split(':')
        else:
            host = self.es_host
            port = '9200'
        es = Elasticsearch(f"http://{host}:{port}",
            http_auth=(self.es_user, self.es_pass)
        )
        return es

    def index_elastic(self, es, index_name,bulk_data):
        mappings = {
            "properties": {
                "id": {"type": "text"},
                "timestamp_kafka": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                "data": {"type": "object"}
            }
        }
        try:
            es.indices.create(index=index_name, mappings=mappings)
        except:
            pass
        
        bulk(es, bulk_data)
        es.indices.refresh(index=index_name)

    def consume_kafka_push_elasticsearch(self):
        consumer = self.connect_kafka()
        es = self.connect_elasticsearch()
        print(f"Start indexing...")
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            else:
                data = json.loads(msg.value().decode('utf-8'))
                try:
                    bulk_data = bulk_data
                except:
                    bulk_data = []
                if 'sniffing' in data.get("media_tags", []):
                    id_x = data.get("id", "")
                    timestamp_kafka = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(msg.timestamp()[1]/1000.))
                    
                    index_date = time.strftime("%Y%m%d", time.localtime())
                    index_name = f"{self.es_index_prefix}-{index_date}"
                    
                    bulk_data.append(
                        {
                            "_index": index_name,
                            "_source": {        
                                "id": id_x,
                                "timestamp_kafka": timestamp_kafka,
                                "data": data
                            }
                        }
                    )
                    if len(bulk_data) >= 20:
                        self.index_elastic(es, index_name, bulk_data)
                        ids = [i.get("id", "") for i in bulk_data]
                        bulk_data = []
                        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}] id {ids} pushed to index {index_name}")
                    

def main():
    run = Logging()
    run.consume_kafka_push_elasticsearch()


if __name__ == "__main__":
    main()
