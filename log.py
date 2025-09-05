# -*- coding:utf-8 -*-
# @FileName  :log.py
# @Time      :2025/9/4 16:45
# @Author    :shi lei.wei  <slwei@eppei.com>.
import logging
import os
from logging.config import dictConfig
from logging.handlers import TimedRotatingFileHandler

import yaml

# 内部变量，不对外暴露
_logger = None


def get_logger(log_name='pec_cloud.log', log_level=logging.INFO):
    global _logger
    if _logger is not None:
        return _logger
    os.makedirs(os.getcwd() + os.sep + "log", exist_ok=True)
    # 初始化日志配置
    file_handler = TimedRotatingFileHandler(
        filename=os.getcwd() + os.sep + "log" + os.sep + log_name,
        when='midnight',  # 按天滚动
        interval=1,  # 每隔1天
        backupCount=365,  # 保留最近365天的日志文件
        encoding='utf-8'  # 日志文件编码
    )
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(name)s %(filename)s %(funcName)s[line:%(lineno)d]%(levelname)s'
               ' %(threadName)s - %(message)s',
        handlers=[
            file_handler,
            logging.StreamHandler()
        ]
    )
    _logger = logging.getLogger("hunter")
    return _logger


def setup_logger(log_dir="log", log_name="app.log"):
    config_path = "log.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    # 动态替换日志路径
    log_file_path = os.path.join(os.getcwd(), log_dir, log_name)
    config["handlers"]["file"]["filename"] = log_file_path
    # 创建日志目录（如果不存在）
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    # 应用日志配置
    dictConfig(config)


if __name__ == "__main__":
    run_code = 0
