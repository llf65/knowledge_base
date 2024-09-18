import os
from urllib.parse import urlparse

parsed_url = urlparse('http://eid.csrc.gov.cn/mnt/storage/stock/pre_ipo/2024/4/27/PREIPO_F0101_V01_20240427_22181_Q/11EA029181D343DB920DA68C8F6A2AE8/关于江西力源海纳科技股份有限公司首次公开发行股票并上市辅导工作进展报告（第一期）.pdf')
file_name = os.path.basename(parsed_url.path)
print(file_name)