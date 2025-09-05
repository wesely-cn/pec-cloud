# -*- coding:utf-8 -*-
# @FileName  :gunicorn.conf.py.py
# @Time      :2025/9/4 16:51
# @Author    :shi lei.wei  <slwei@eppei.com>.

# gunicorn.conf.py
import multiprocessing

# 服务器配置
bind = "0.0.0.0:6100"
workers = multiprocessing.cpu_count() * 2 + 1  # 工作进程数
worker_class = "sync"  # 同步工作模式
worker_connections = 1000  # 每个工作进程的最大并发连接数
timeout = 30  # 请求超时时间
keepalive = 2  # Keep-Alive时间

# 日志配置
accesslog = "/app/log/gunicorn/access.log"
errorlog = "/app/log/gunicorn/error.log"
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# 进程管理
preload_app = True  # 预加载应用
max_requests = 1000  # 每个工作进程处理1000个请求后重启
max_requests_jitter = 100  # 随机抖动，避免所有进程同时重启

# 安全配置
limit_request_line = 4094  # HTTP请求行最大长度
limit_request_fields = 100  # 最大HTTP请求头字段数
limit_request_field_size = 8190  # 最大HTTP请求头字段大小
