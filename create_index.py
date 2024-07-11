from elasticsearch7 import Elasticsearch
from connect import connect

host = "10.100.1.71:9200"
username = "elastic"
password = "rahasia2024"

es = connect(host, username, password)



import pandas as pd

df = (
    pd.read_csv("wiki_movie_plots_deduped.csv")
    .dropna()
    .sample(5000, random_state=42)
    .reset_index()
)



settings = {
        "index": {
            "number_of_shards": "1",
            "number_of_replicas": "0"
        }
}

mappings = {
        "properties": {
            "title": {"type": "text", "analyzer": "english"},
            "ethnicity": {"type": "text", "analyzer": "standard"},
            "director": {"type": "text", "analyzer": "standard"},
            "cast": {"type": "text", "analyzer": "standard"},
            "genre": {"type": "text", "analyzer": "standard"},
            "plot": {"type": "text", "analyzer": "english"},
            "year": {"type": "integer"},
            "wiki_page": {"type": "keyword"}
    }
}



from elasticsearch7.helpers import bulk

for no in range(1,21):
    index = f"movies-{no}"
    bulk_data = []
    es.indices.create(index=index, mappings=mappings, settings=settings)
    for i,row in df.iterrows():
        bulk_data.append(
            {
                "_index": index,
                "_id": i,
                "_source": {        
                    "title": row["Title"],
                    "ethnicity": row["Origin/Ethnicity"],
                    "director": row["Director"],
                    "cast": row["Cast"],
                    "genre": row["Genre"],
                    "plot": row["Plot"],
                    "year": row["Release Year"],
                    "wiki_page": row["Wiki Page"],
                }
            }
        )
    
    bulk(es, bulk_data)
    es.indices.refresh(index=index)
    print(es.cat.count(index=index, format="json"))
