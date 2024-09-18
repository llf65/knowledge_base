# run_spider.py

import sys
import os
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings


def run_spider():
    # Add the project directory to sys.path
    project_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(project_dir)

    # Set the environment variable for Scrapy settings
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'knowledge_base.settings')

    # Import the settings module
    from knowledge_base import settings as project_settings
    # Import your spider
    from knowledge_base.spiders.CsrcSpider import CsrcSpider

    # Create a Settings object and set the settings module
    crawler_settings = Settings()
    crawler_settings.setmodule(project_settings)

    # Initialize the crawler process with the settings
    process = CrawlerProcess(settings=crawler_settings)
    process.crawl(CsrcSpider)
    process.start()


if __name__ == '__main__':
    run_spider()
