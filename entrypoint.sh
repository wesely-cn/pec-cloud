#!/bin/bash
set -e

echo "=== 数据发布服务启动 ==="
echo "时间: $(date)"
echo "环境: ${FLASK_ENV:-production}"

# 执行初始化脚本
echo "执行初始化..."
/app/init.sh

# 等待依赖服务（如果需要）
if [ -n "$WAIT_FOR_HOST" ] && [ -n "$WAIT_FOR_PORT" ]; then
    echo "等待依赖服务启动: $WAIT_FOR_HOST:$WAIT_FOR_PORT"
    /app/scripts/wait-for-it.sh "$WAIT_FOR_HOST" "$WAIT_FOR_PORT" -- echo "依赖服务已启动"
fi

# 启动应用
echo "启动Gunicorn服务器..."
exec gunicorn \
    --bind "0.0.0.0:6100" \
    --workers 4 \
    --worker-class sync \
    --timeout 30 \
    --keep-alive 2 \
    --max-requests 1000 \
    --max-requests-jitter 100 \
    --log-level info \
    --access-logfile /app/log/gunicorn-access.log \
    --error-logfile /app/log/gunicorn-error.log \
    --preload \
    zeromq_server:gun_app