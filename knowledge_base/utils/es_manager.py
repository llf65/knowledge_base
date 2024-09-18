# knowledge_base/utils/es_manager.py

from elasticsearch import Elasticsearch
from knowledge_base.config.es_config import es_config


class ElasticsearchManager:
    def __init__(self):
        self.es = Elasticsearch(
            hosts=[{
                'host': es_config['es_host'],
                'port': es_config['es_port'],
                'scheme': es_config['es_scheme']
            }],
            http_auth=(es_config['es_username'], es_config['es_password']),
            ca_certs=es_config['ca_cert_path']
        )

    def index_document(self, index_name, document, doc_id=None):
        """
        将文档索引到指定的Elasticsearch索引中.

        :param index_name: Name of the index.
        :param document: The document to index (dictionary).
        :param doc_id: Optional document ID.
        """
        try:
            self.es.index(index=index_name, id=doc_id, document=document)
            return True
        except Exception as e:
            # Log the error as needed
            return False
