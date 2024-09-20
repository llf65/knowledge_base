# knowledge_base/utils/FileUploader.py
import logging
import time

import requests
import json

from knowledge_base.config.FileUploaderConfig import api_config


class FileUploader:
    def __init__(self, strategy):
        self.strategy = strategy
        self.logger = logging.getLogger(__name__)

    def upload_file_to_wps(self, file_obj, item, max_retries=3, backoff_factor=1):
        upload_url = api_config['upload_wps_url']
        headers = api_config['upload_headers']
        data = api_config['upload_data']

        # 生成文件名和MIME类型
        file_name = self.strategy.get_file_name(item)
        mime_type = self.strategy.get_mime_type(item)

        for attempt in range(max_retries):
            try:
                # 每次尝试前将文件指针重置到开头
                file_obj.seek(0)

                # Prepare file info
                files = {'file': (file_name, file_obj, mime_type)}

                # 发送POST请求以上传文件
                response = requests.post(upload_url, headers=headers, files=files, data=data)
                if response.status_code == 200:
                    try:
                        response_data = response.json()
                        if response_data['code'] == '0' and 'fileId' in response_data['body']['bizParam']:
                            fileId = response_data['body']['bizParam']['fileId']
                            return fileId
                        else:
                            self.logger.error(f"Upload failed for {file_name}. Response: {response.text}")
                    except json.JSONDecodeError:
                        self.logger.error(f"Cannot parse JSON response for {file_name}. Response: {response.text}")
                else:
                    self.logger.error(
                        f"Upload failed for {file_name}. Status code: {response.status_code}, Response: {response.text}")
            except requests.RequestException as e:
                self.logger.error(f"Request exception when uploading {file_name}. Error: {str(e)}")

            # 如果上传失败，等待重试
            if attempt < max_retries - 1:
                sleep_time = backoff_factor * (2 ** attempt)
                self.logger.info(f"Retrying upload for {file_name} in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                self.logger.error(f"Failed to upload {file_name} after {max_retries} attempts.")
                return None
