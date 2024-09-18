import logging
import os
from urllib.parse import urlparse, quote, urlunparse

from scrapy.exceptions import DropItem
from twisted.internet.defer import Deferred
from twisted.internet import threads

from knowledge_base.config.csrc_api_config import API_PARAMS
from knowledge_base.utils.FileStrategy import CsrcFileStrategy
from knowledge_base.utils.TextExtractor import TextExtractor
from knowledge_base.utils.db_manager import DatabaseManager
from knowledge_base.utils.es_manager import ElasticsearchManager
from knowledge_base.utils.download_utils import download_file
from knowledge_base.utils.FileUploader import FileUploader


class CsrcPipeline:
    def __init__(self):
        self.download_dir = os.path.join(os.getcwd(), 'downloads')
        os.makedirs(self.download_dir, exist_ok=True)
        self.logger = logging.getLogger(__name__)

    # 特殊字符被正确编码
    def encode_url(self, url):
        parsed_url = urlparse(url)
        encoded_path = quote(parsed_url.path)
        encoded_query = quote(parsed_url.query, safe="=&")
        encoded_url = urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            encoded_path,
            parsed_url.params,
            encoded_query,
            parsed_url.fragment
        ))
        return encoded_url

    def process_item(self, item, spider):
        deferred = Deferred()

        def _process():
            # Step 1: Check if item exists
            db_manager = DatabaseManager()
            es_manager = ElasticsearchManager()
            file_uploader = FileUploader(strategy=CsrcFileStrategy())
            if db_manager.check_item_exists(
                    table_name='t_knowledge_content_guidance',
                    condition={'source_id': item['source_file_id']}
            ):
                raise DropItem(f"Item {item['source_file_id']} already exists.")

            # Step 2: Download file
            url_extension = os.path.splitext(urlparse(item['pdf_url']).path)[1]
            file_name = item['report_title'].replace('/', '_').replace('\\', '_') + url_extension  # Clean the file name
            headers = API_PARAMS['HEADERS'].copy()
            cookies = API_PARAMS['COOKIES']
            encoded_url = self.encode_url(item['pdf_url'])
            raw_referer = item.get('page_url', 'http://eid.csrc.gov.cn/csrcfd/index_1_f.html')
            headers['Referer'] = self.encode_url(raw_referer)

            download_success = download_file(
                encoded_url,
                self.download_dir,
                file_name=file_name,
                headers=headers,
                cookies=cookies
            )
            if not download_success:
                self.record_failure(db_manager, item, reason="Download failed")
                return

            file_path = download_success

            # Step 3: Upload to WPS server
            with open(file_path, 'rb') as file_obj:
                file_id = file_uploader.upload_file_to_wps(file_obj, item)
            if not file_id:
                self.record_failure(db_manager, item, reason="Upload failed")
                return

            item['file_id'] = file_id

            # Step 4: Insert into MySQL
            db_success = self.insert_into_database(db_manager, item)
            if not db_success:
                return

            # Step 5: Extract text and insert into Elasticsearch
            file_ext = os.path.splitext(file_path)[1].lstrip('.').lower()
            with open(file_path, 'rb') as file_obj:
                text_content = TextExtractor.extract_text(file_obj, file_ext)
            if text_content:
                text_content = text_content.replace("\r\n", "\n").replace("\r", "\n")
                # TODO 标签相关逻辑暂未定义
                es_doc = self.construct_es_doc(item, text_content)
                self.es_manager.index_document(index_name='knowledge_guidance', document=es_doc)
            else:
                self.record_failure(db_manager, item, reason="Text extraction failed")

            return item

        threads.deferToThread(_process).addCallback(deferred.callback)
        return deferred

    def insert_into_database(self, db_manager, item):
        try:
            # Begin transaction
            db_manager.connection.begin()

            # Insert into t_knowledge
            knowledge_data = {
                'knowledge_id': item['knowledge_id'],
                'knowledge_sort': item['knowledge_sort'],
                # Add other fields as needed
            }
            db_manager.insert_into_table('t_knowledge', knowledge_data)

            # Insert into t_knowledge_content
            content_data = {
                'knowledge_id': item['knowledge_id'],
                'logic_knowledge_id': item['logic_knowledge_id'],
                'guide_code': item['guide_code'],
                # Add other fields as needed
            }
            db_manager.insert_into_table('t_knowledge_content', content_data)

            # Insert into t_knowledge_content_guidance
            guidance_data = {
                'logic_knowledge_id': item['logic_knowledge_id'],
                'serial_no': item['serial_no'],
                'belong_secu': item['belong_secu'],
                'source_id': item['source_file_id'],
                # Add other fields as needed
            }
            db_manager.insert_into_table('t_knowledge_content_guidance', guidance_data)

            # Commit transaction
            db_manager.connection.commit()
            return True
        except Exception as e:
            # Rollback transaction
            db_manager.connection.rollback()
            # Log the error
            self.record_failure(item, str(e))
            return False

    def construct_es_doc(self, item, text_content):
        es_doc = {
            'logic_knowledge_id': item['logic_knowledge_id'],
            'serial_no': item['serial_no'],
            'belong_secu': item.get('agency', ''),
            'guidance_status': item.get('project_status', ''),
            'company_name': item.get('company_name', ''),
            'company_simple_name': '',  # If available
            'area': '',  # If available
            'file_id': item['file_id'],
            'file_name': item.get('file_name', ''),
            'notice_type_name': item.get('report_type', ''),
            'guide_name': item.get('report_title', ''),
            'summary': '',  # If available
            'guide_content': text_content,
            'rele_date': item.get('filing_date', ''),
            'create_time': '',  # Set appropriately
            'update_time': '',  # Set appropriately
            'create_by': 'system',
            'update_by': 'system',
        }
        return es_doc

    def record_failure(self, db_manager, item, reason):
        failure_data = {
            'title': item.get('report_title', ''),
            'source': 'CSRC',
            'type': 1,  # Assuming type 1 for guidance information
            'reason': reason,
        }
        db_manager.insert_into_table('t_Download_failed', failure_data)
