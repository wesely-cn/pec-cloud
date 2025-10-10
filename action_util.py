import datetime
import logging
import re

import requests

from config_manager import ConfigManager

logger = logging.getLogger(__name__)


def extract_date(filename):
    # 使用正则表达式匹配日期
    match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
    if match:
        date_str = match.group(0)  # 获取匹配到的第一个日期字符串
        logger.info("提取的日期为: %s, %s", filename, date_str)
        return date_str
    else:
        logger.error("未找到日期: %s", filename)
        return datetime.datetime.now().strftime("%Y-%m-%d")


def call_third_api(payload, **kwargs):
    """调用接口，传输文件(流方式)"""
    try:
        if int(kwargs.get('push_data', 1)) == 0:
            return
        the_host = ConfigManager.get_init_param_by_key("third_host", "http://10.184.37.90")
        the_path = ConfigManager.get_init_param_by_key("third_top_path", "/monitor/crawler/parseTopData")
        default_headers = {
            'Content-Type': 'application/json',
            'crawler-code': 'gs-znzx-fd-crawler'
        }
        header = kwargs.get('_header', default_headers)
        timeout = kwargs.get("timeout", (30, 30))
        response = requests.post(
            url=the_host + the_path,
            headers=header,
            data=payload,
            timeout=timeout
        )
        rs = str(response.json())
        if response.status_code != requests.codes.ok:
            raise Exception(rs)
        if not response.json().get("success"):
            raise Exception(rs)
        logger.info(rs)
        return rs
    except Exception as e:
        logger.exception(e)
        return "call api failed"


if __name__ == '__main__':
    payload1 = {
        "intervalDays": 4,
        "skipDate": 0,
        "dateList": ["2025-05-18", "2025-05-18"]
    }
    call_third_api(payload1)
