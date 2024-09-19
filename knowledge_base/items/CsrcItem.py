# knowledge_base/items/CsrcItem.py
import scrapy


# 定义爬取的数据结构（Item）# 辅导信息_中国证劵监督管理委员会网上办事服务平台（试运行）
class CsrcItem(scrapy.Item):
    company_name = scrapy.Field()
    securities_company = scrapy.Field()
    filing_date = scrapy.Field()
    project_status = scrapy.Field()
    agency = scrapy.Field()
    report_type = scrapy.Field()
    report_title = scrapy.Field()
    pdf_url = scrapy.Field()
    source_file_id = scrapy.Field()
    file_id = scrapy.Field()
    logic_knowledge_id = scrapy.Field()
    serial_no = scrapy.Field()

