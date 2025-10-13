import datetime
import logging
import re
import time

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


def push_with_retry(data):
    """执行推送并支持重试"""
    max_retries = ConfigManager.get_init_param_by_key("max_push_retries", 5)
    retry_delay = ConfigManager.get_init_param_by_key("retry_delay", 600)
    task_name = data.get('task_name', 'N/A')
    inner_payload = data.get('payload', 'N/A')
    sequence = inner_payload.get('sequence', 'N/A')
    for attempt in range(max_retries + 1):
        try:
            push_success = call_third_api(data)
            if push_success:
                if attempt > 0:
                    logger.info(f"推送成功 [{task_name}][{sequence}] (重试 {attempt} 次)")
                else:
                    logger.info(f"推送成功 [{task_name}][{sequence}]")
                return True
            else:
                logger.warning(f"推送失败 [{task_name}][{sequence}] (尝试 {attempt + 1}/{max_retries + 1})")
        except Exception as e:
            logger.error(f"推送异常 [{task_name}][{sequence}] (尝试 {attempt + 1}/{max_retries + 1}): {e}")
        # 如果不是最后一次尝试，等待后重试
        if attempt < max_retries:
            # 指数退避
            retry_delay_time = retry_delay * (2 ** attempt)
            time.sleep(retry_delay_time)
    logger.error(f"推送最终失败 [{task_name}][{sequence}]，已重试 {max_retries} 次")
    return False


def call_third_api(data, **kwargs):
    """调用接口，传输文件(流方式)"""
    if int(kwargs.get('push_data', 1)) == 0:
        return
    # 外层对象：{"task_name": task_name, "payload": inner_payload}
    task_name = data.get('task_name', 'N/A')
    inner_payload = data.get('payload', 'N/A')
    the_host = ConfigManager.get_init_param_by_key("third_host", "http://10.184.37.90/api")
    top_path = ConfigManager.get_init_param_by_key("third_top_path", "/monitor/crawler/parseTopData")
    deal_path = ConfigManager.get_init_param_by_key("third_deal_path", "/monitor/crawler/parseDealData")
    if "_top" in task_name:
        the_path = top_path
    else:
        the_path = deal_path
    default_headers = {
        'Content-Type': 'application/json',
        'crawler-code': 'gs-znzx-fd-crawler'
    }
    header = kwargs.get('_header', default_headers)
    logger.debug("post payload: %s", data)
    payload = inner_payload.get('data', {})
    timeout = kwargs.get("timeout", (30, 30))
    response = requests.post(
        url=the_host + the_path,
        headers=header,
        json=payload,
        timeout=timeout
    )
    rs = str(response.json())
    if response.status_code != requests.codes.ok:
        raise Exception(rs)
    if not response.json().get("success"):
        raise Exception(rs)
    logger.info(rs)
    return True


if __name__ == '__main__':
    pass
