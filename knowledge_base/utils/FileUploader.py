import logging

import requests
import json

from knowledge_base.config.FileUploaderConfig import api_config


class FileUploader:
    def __init__(self, strategy):
        self.strategy = strategy
        self.logger = logging.getLogger(__name__)


    def upload_file_to_wps(self, file_obj, item):
        upload_url = api_config['upload_wps_url']
        headers = api_config['upload_headers']
        data = api_config['upload_data']

        # 构建文件名和设置正确的MIME类型
        file_name = self.strategy.get_file_name(item)
        mime_type = self.strategy.get_mime_type(item)

        # 准备文件信息，使用BytesIO对象
        files = {'file': (file_name, file_obj, mime_type)}

        # 发起POST请求上传文件
        try:
            response = requests.post(upload_url, headers=headers, files=files, data=data)
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    if response_data['code'] == '0' and 'fileId' in response_data['body']['bizParam']:
                        fileId = response_data['body']['bizParam']['fileId']
                        return fileId
                    else:
                        return None
                except json.JSONDecodeError:
                    self.logger.error(f"无法解析JSON响应。原始响应内容: {response.text}")
                    return None
            else:
                self.logger.error(f"上传文件 {file_name} 失败。状态码: {response.status_code}, 响应内容: {response.text}")
                return None
        except requests.RequestException as e:
            self.logger.error(f"请求异常，上传文件 {file_name} 失败。错误信息: {str(e)}")
            return None
