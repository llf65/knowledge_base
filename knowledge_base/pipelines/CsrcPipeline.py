# knowledge_base/pipelines/CsrcPipeline.py
import logging
import os
from urllib.parse import urlparse, quote, urlunparse

from scrapy.exceptions import DropItem
from twisted.internet.defer import Deferred
from twisted.internet import threads

from knowledge_base.config.csrc_api_config import API_PARAMS
from knowledge_base.constants.mappings import belong_secu_code_MAPPING, agency_area_MAPPING
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
        # self.es_manager = ElasticsearchManager()  # Ensure es_manager is initialized

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
                self.record_failure(db_manager, item, reason="Download failed", fail_type='1')
                return

            file_path = download_success

            # Step 3: Upload to WPS server
            with open(file_path, 'rb') as file_obj:
                fileId = file_uploader.upload_file_to_wps(file_obj, item)
                print('file_id-->>>>', fileId)
            if not fileId:
                self.record_failure(db_manager, item, reason="Upload failed", fail_type='2')
                return

            item['file_id'] = fileId

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
                es_manager.index_document(index_name='knowledge_guidance', document=es_doc)
            else:
                self.record_failure(db_manager, item, reason="Text extraction failed", fail_type='4')

            return item

        threads.deferToThread(_process).addCallback(deferred.callback)
        return deferred

    def insert_into_database(self, db_manager, item):
        try:
            # Begin transaction
            db_manager.connection.begin()

            # Insert into t_knowledge
            knowledge_data = {
                'knowledge_sort': '174014',
                'mannual_yn': '524002',
                'regi_state': '010001',
                'create_by': 'admin'
                # Add other fields as needed
            }
            knowledge_id = db_manager.insert_into_table('t_knowledge', knowledge_data)

            # Insert into t_knowledge_content
            content_data = {
                'knowledge_id': knowledge_id,
                # Add other fields as needed
                'guide_code': 'UTF-8',  # 知识编码
                'guide_name': item['report_title'],  # 知识标题
                # 'guide_content': item['report_title'],  # 知识内容
                'source': item['pdf_url'],  # 来源出处
                # 'version': item['source_file_id'],  # 版本
                'publication_state': '394002',  # 发布状态
                'rele_date': item['filing_date'],  # 发布日期
                'allow_comment_yn': '395002',  # 是否允许评论
                'need_check_yn': '649001',  # 是否需要审核
                'examine_status': '178001',  # 审核状态
                # 'examine_person_id': item['source_file_id'],  # 审核人ID
                'law_status': '831001',  # 效力状态
                'regi_state': '010001',  # 记录状态
                'create_by': 'admin'  # 创建用户
            }
            logic_knowledge_id = db_manager.insert_into_table('t_knowledge_content', content_data)

            # Store logic_knowledge_id in item
            item['logic_knowledge_id'] = logic_knowledge_id

            # Insert into t_knowledge_content_guidance
            guidance_data = {
                'logic_knowledge_id': logic_knowledge_id,
                'belong_secu': belong_secu_code_MAPPING.get(item['agency'], ''),  # 所属监管局
                # Add other fields as needed
                'guidance_status': item['project_status'],  # 辅导状态
                'company_name': item['company_name'],  # 公司名称
                # 'company_simple_name': item['source_file_id'], # 公司简称
                'area': agency_area_MAPPING.get(item['agency'], ''),  # 地域分布
                # 'stock_code': item['source_file_id'], # 股票编码
                'notice_type_name': item['report_type'],  # 公告类型名称
                'file_id': item['file_id'],  # 文件ID
                'file_name': item['report_title'],  # 文件名称
                'source_id': item['source_file_id'],  # 源ID
                'regi_state': '010001',  # 记录状态
                'create_by': 'admin'  # 创建用户
            }
            # db_manager.insert_into_table('t_knowledge_content_guidance', guidance_data)
            serial_no = db_manager.insert_into_table('t_knowledge_content_guidance', guidance_data)

            print("guidance_data:>>>>>>>>>>>>>>", guidance_data)

            # Store serial_no in item
            item['serial_no'] = serial_no

            # Commit transaction
            db_manager.connection.commit()
            return True
        except Exception as e:
            # Rollback transaction
            db_manager.connection.rollback()
            # Log the error
            self.record_failure(db_manager, item, reason=str(e), fail_type='3')
            return False

    def construct_es_doc(self, item, text_content):
        es_doc = {
            'logic_knowledge_id': item['logic_knowledge_id'],  # 逻辑知识ID
            'serial_no': item['serial_no'],  # t_knowledge_content_guidance表的主键
            'belong_secu': belong_secu_code_MAPPING.get(item['agency'], ''),  # 所属监管局
            'guidance_status': item.get('project_status', ''),  # 辅导状态
            'company_name': item.get('company_name', ''),  # 公司名称
            # 'company_simple_name': '',  # If available # 公司简称
            'area': agency_area_MAPPING.get(item['agency'], ''),  # If available # 地域分布
            'file_id': item['file_id'],  # 文件ID
            'file_name': item.get('report_title', ''),  # 文件名称
            'notice_type_name': item.get('report_type', ''),  # 公告类型名称
            'guide_name': item.get('report_title', ''),  # 知识标题
            # 'summary': '',  # If available # 逻辑知识ID
            'guide_content': text_content,  # 文档内容
            'rele_date': item.get('filing_date', ''),  # 发布日期
            'create_time': '',  # Set appropriately
            'update_time': '',  # Set appropriately
            'create_by': 'admin',
            'update_by': 'admin',
        }
        return es_doc

    def record_failure(self, db_manager, item, reason, fail_type):
        failure_data = {
            'title': item.get('report_title', ''),
            'source': 'CSRC',
            'file_type': 1,  # Assuming type 1 for guidance information
            'reason': reason,
            'fail_type': fail_type,
            'regi_state': '001001',
            'create_by': 'admin'
        }
        db_manager.insert_into_table('t_failed', failure_data)
