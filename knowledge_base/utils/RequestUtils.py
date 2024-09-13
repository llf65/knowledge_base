# utils/request_utils.py
import scrapy
from scrapy import FormRequest


class RequestUtils:
    @staticmethod
    def create_post_request(url, formdata, headers, cookies, callback):
        return FormRequest(
            url=url,
            formdata=formdata,
            headers=headers,
            cookies=cookies,
            callback=callback
        )
