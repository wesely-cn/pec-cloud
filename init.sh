#!/bin/bash
set -e

echo "=== 数据发布服务初始化 ==="

# 创建必要的目录
echo "创建日志目录..."
mkdir -p /app/log
mkdir -p /app/data

# 设置权限
echo "设置目录权限..."
chmod -R 755 /app/log
chmod -R 755 /app/data

# 初始化日志文件
echo "初始化日志文件..."
touch /app/log/gunicorn.log
chmod 644 /app/log/*.log

cd /app
# 验证环境变量
echo "验证环境变量..."
if [ -z "$FLASK_ENV" ]; then
    export FLASK_ENV="production"
fi

echo "环境变量 FLASK_ENV: $FLASK_ENV"

# 显示配置摘要
echo "=== 配置摘要 ==="
echo "监听地址: 0.0.0.0:6100"
echo "ZMQ地址: tcp://0.0.0.0:6666"
echo "日志目录: /app/log"
echo "数据目录: /app/data"
echo "================"

echo "初始化完成"