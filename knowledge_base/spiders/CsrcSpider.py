# knowledge_base/spiders/CsrcSpider.py
import hashlib
import re

import scrapy

from knowledge_base.config.csrc_api_config import API_PARAMS
from knowledge_base.utils.RequestUtils import RequestUtils
from knowledge_base.items.CsrcItem import CsrcItem


class CsrcSpider(scrapy.Spider):
    name = 'csrcspider'

    def __init__(self, *args, **kwargs):
        super(CsrcSpider, self).__init__(*args, **kwargs)
        self.page_number = 1  # 初始页码
        # self.custom_settings = get_project_settings()  # 获取settings配置

    def start_requests(self):
        # 从 settings.py 中读取配置
        base_url = API_PARAMS['BASE_URL']
        formdata = API_PARAMS['FORMDATA']
        headers = API_PARAMS['HEADERS']
        cookies = API_PARAMS['COOKIES']
        # base_url = self.custom_settings.get('BASE_URL')
        # formdata = self.custom_settings.get('FORMDATA')
        # headers = self.custom_settings.get('HEADERS')
        # cookies = self.custom_settings.get('COOKIES')

        url = base_url.format(self.page_number)

        # 调用封装后的请求方法
        yield RequestUtils.create_post_request(url, formdata, headers, cookies, self.parse)

    def parse(self, response):
        # 获取表格中的行，跳过第一行（标题行）
        rows = response.xpath('//table[@class="m-table2 m-table2-0"]/tr[position()>1]')
        # 如果有效数据行仍为空，停止爬取
        if not rows:
            self.logger.info(f"No valid data found on page {self.page_number}. Stopping crawler.")
            return  # 没有数据，停止爬虫

        previous_company_name = None  # 用于保存最近一次有效的公司名称

        for row in rows:
            item = CsrcItem()
            # 提取公司名称，判断是否为空，如果为空，并且 previous_company_name 有值，则继承
            company_name = row.xpath('td[2]/text()').get()
            if company_name and company_name.strip():  # 如果公司名称存在且不为空字符串
                company_name = company_name.strip()  # 去除左右空格
                previous_company_name = company_name  # 更新最近的有效公司名称
            else:
                # 如果 company_name 是空字符串或者 None，使用前面保存的有效 company_name
                company_name = previous_company_name  # 继承最近的有效公司名称

            # # 提取证券公司名称
            # securities_company = row.xpath('td[3]/text()').get()
            # if securities_company:
            #     securities_company = securities_company.strip()
            #
            # # 提取备案时间
            # filing_date = row.xpath('td[4]/text()').get()
            # if filing_date:
            #     filing_date = filing_date.strip()
            #
            # # 提取辅导状态
            # project_status = row.xpath('td[5]/text()').get()
            # if project_status:
            #     project_status = project_status.strip()
            #
            # # 提取派出机构
            # agency = row.xpath('td[6]/text()').get()
            # if agency:
            #     agency = agency.strip()
            #
            # # 提取报告类型
            # report_type = row.xpath('td[7]/text()').get()
            # if report_type:
            #     report_type = report_type.strip()
            item['company_name'] = company_name
            item['securities_company'] = row.xpath('td[3]/text()').get().strip() if row.xpath('td[3]/text()').get() else None
            item['filing_date'] = row.xpath('td[4]/text()').get().strip() if row.xpath('td[4]/text()').get() else None
            item['project_status'] = row.xpath('td[5]/text()').get().strip() if row.xpath('td[5]/text()').get() else None
            item['agency'] = row.xpath('td[6]/text()').get().strip() if row.xpath('td[6]/text()').get() else None
            item['report_type'] = row.xpath('td[7]/text()').get().strip() if row.xpath('td[7]/text()').get() else None
            # item['report_title'] = row.xpath('td[8]/text()').get().strip() if row.xpath('td[8]/text()').get() else None
            item['report_title'] = row.xpath('td[8]/@title').get() or (
                row.xpath('td[8]/text()').get().strip() if row.xpath('td[8]/text()').get() else None)

            # Clean up extracted fields
            for field in ['securities_company', 'filing_date', 'project_status', 'agency', 'report_type',
                          'report_title']:
                if item[field]:
                    item[field] = item[field].strip()
                else:
                    item[field] = None

            # 获取PDF下载链接
            pdf_url = None

            # 优先从 <td> 中获取 pdf_url
            onclick_text = row.xpath('td[2]/@onclick').get()
            if onclick_text:
                pdf_match = re.search(r"downloadPdf1\('([^']+)", onclick_text)
                if pdf_match:
                    pdf_url = pdf_match.group(1)

            # 如果 <td> 中未找到 pdf_url，则尝试从 <tr> 中获取 pdf_url
            if not pdf_url:
                tr_onclick_text = row.xpath('@onclick').get()
                if tr_onclick_text:
                    pdf_match = re.search(r"downloadPdf1\('([^']+)", tr_onclick_text)
                    if pdf_match:
                        pdf_url = pdf_match.group(1)

            # 拼接 pdf_url
            if pdf_url:
                item['pdf_url'] = 'http://eid.csrc.gov.cn' + pdf_url

            report_title = item.get('report_title')
            if report_title:
                report_title = report_title.strip()
                item['report_title'] = report_title
                # Generate hash
                hash_object = hashlib.md5(report_title.encode('utf-8'))
                source_file_id = hash_object.hexdigest()
                item['source_file_id'] = source_file_id
            else:
                self.logger.warning(f"Missing report_title for item: {item}")
                continue  # or set a default value
            # 提取报告标题
            # report_title = row.xpath('td[8]/text()').get()
            # if report_title:
            #     item['report_title'] = report_title.strip()
            yield item
            # # 输出提取到的有效数据
            # yield {
            #     'company_name': company_name,
            #     'securities_company': securities_company,
            #     'filing_date': filing_date,
            #     'project_status': project_status,
            #     'agency': agency,
            #     'report_type': report_type,
            #     'report_title': report_title,
            #     'pdf_url': pdf_url
            # }
            # 处理完当前页面后，尝试访问下一页
        self.page_number += 1
        next_url = API_PARAMS['BASE_URL'].format(self.page_number)
        # 发送下一页请求
        yield RequestUtils.create_post_request(next_url, API_PARAMS['FORMDATA'],
                                               API_PARAMS['HEADERS'], API_PARAMS['COOKIES'],
                                               self.parse)


if __name__ == "__main__":
    from scrapy.crawler import CrawlerProcess

    process = CrawlerProcess()
    process.crawl(CsrcSpider)
    process.start()
