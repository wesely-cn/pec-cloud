#!/bin/bash

# 创建日志目录
mkdir -p /var/log/gunicorn

# 启动Gunicorn
exec gunicorn --config gunicorn.conf.py server:app