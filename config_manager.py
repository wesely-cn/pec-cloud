# -*- coding:utf-8 -*-
# @FileName  :config_util.py
# @Time      :2024/11/22 20:17
# @Author    :shi lei.wei  <slwei@eppei.com>.

import json
import os
import sys


class ConfigManager:
    _data = None
    _param = {}

    @staticmethod
    def load_config(file_path):
        if ConfigManager._data is not None:
            return

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Config file not found at {file_path}")

        with open(file_path, 'r', encoding='utf-8') as file:
            ConfigManager._data = json.load(file)
        for key, value in ConfigManager._data.items():
            ConfigManager._param.setdefault(key, value)
        return ConfigManager._data

    @staticmethod
    def get_init_param_by_key(key, default_value=None):
        param_value = ConfigManager._param.get(key)
        the_value = param_value if param_value else default_value
        return the_value

    @staticmethod
    def get_param_by_key(key, default_value=None):
        if key in ConfigManager._data:
            return ConfigManager._data.get(key)
        else:
            return default_value


def load_config(filename='config.json'):
    if getattr(sys, 'frozen', False):
        # 如果是打包后的应用程序
        config_file_path = sys._MEIPASS + os.sep + filename
    else:
        # 如果是开发环境
        config_file_path = os.path.abspath(".") + os.sep + filename
    return ConfigManager.load_config(config_file_path)


if __name__ == "__main__":
    load_config()
    ftp_zip = ConfigManager.get_init_param_by_key('retry_delays')
    print(ftp_zip)
