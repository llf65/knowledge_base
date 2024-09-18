# knowledge_base/utils/download_utils.py

import os
import time

import requests
from urllib.parse import urlparse


def download_file(url, download_dir, file_name=None, headers=None, cookies=None):
    """
    从给定的 URL 下载文件并保存到指定的目录。

    :param cookies: 请求头。
    :param headers: 请求的 Cookies。
    :param url: 要下载的文件的 URL。
    :param download_dir: 文件将保存到的目录。
    :param file_name: 可选参数。保存文件的名称。如果未提供，则使用 URL 中的名称。
    :return: 下载文件的路径，如果下载失败则返回 None。
    """

    # 如果下载目录不存在，创建它
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # 如果没有提供文件名，则从 URL 中提取
    if not file_name:
        parsed_url = urlparse(url)
        file_name = os.path.basename(parsed_url.path)

    file_path = os.path.join(download_dir, file_name)

    response = download_file_with_retry(url, headers=headers, cookies=cookies)

    if response:
        try:
            # 将内容写入文件
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"下载成功的file_path: {file_path}")
            print(f"下载成功的url: {url}")
            return file_path  # 返回下载的文件路径
        except Exception as e:
            print(f"保存文件时出错：{e}")
            return None
    else:
        print(f"Failed to download file after retries: {url}")
        return None


def download_file_with_retry(url, headers=None, cookies=None, retries=3, backoff_factor=1):
    """
    尝试多次下载文件，处理临时错误，例如502 Bad Gateway。

    :param url: 要下载的文件的 URL。
    :param headers: 请求头。
    :param cookies: 请求的 Cookies。
    :param retries: 最大重试次数。
    :param backoff_factor: 等待时间的基数，用于指数退避。
    :return: 响应对象或 None（如果失败）。
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, cookies=cookies, timeout=30)
            response.raise_for_status()  # 引发HTTP错误的异常
            return response
        except requests.exceptions.HTTPError as e:
            # 如果是5xx错误，考虑重试
            if 500 <= response.status_code < 600:  # 服务器错误（Server Error）
                print(f"服务器错误（{response.status_code}），重试 {attempt + 1}/{retries} 次...")
            else:
                # 对于其他HTTP错误，不重试
                print(f"HTTP错误：{e}")
                return None
        except requests.exceptions.RequestException as e:
            # 处理其他请求异常，不重试
            print(f"请求异常：{e}")
            return None

        # 计算等待时间，并进行指数退避
        sleep_time = backoff_factor * (2 ** attempt)
        time.sleep(sleep_time)

    print(f"在 {retries} 次尝试后无法下载文件：{url}")
    return None
