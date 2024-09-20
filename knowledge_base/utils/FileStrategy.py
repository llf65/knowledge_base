# knowledge_base/utils/FileStrategy.py
import mimetypes
import os
from abc import ABC, abstractmethod
from urllib.parse import urlparse


class FileStrategy(ABC):
    @abstractmethod
    def get_file_name(self, item):
        pass

    @abstractmethod
    def get_mime_type(self, item):
        pass

    @abstractmethod
    def get_file_extension(self, item):
        pass


# 文件名称类型拓展策略
class CsrcFileStrategy(FileStrategy):  # Csrc-辅导信息获取文件名称类型拓展策略
    def get_file_name(self, item):
        file_extension = self.get_file_extension(item['pdf_url'])
        report_title = item.get('report_title', ' ')
        return f"{report_title}.{file_extension}"

    def get_mime_type(self, item):
        file_extension = self.get_file_extension(item['pdf_url'])
        mime_type, _ = mimetypes.guess_type(f"file.{file_extension}")
        return mime_type or 'application/octet-stream'

    def get_file_extension(self, url):
        parsed_url = urlparse(url)
        return os.path.splitext(parsed_url.path)[1].lstrip('.').lower()
