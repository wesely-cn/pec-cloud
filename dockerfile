FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . /app

# 复制启动脚本
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# 创建必要的目录和设置权限
RUN mkdir -p /app/logs /app/data && \
    chmod +x /app/entrypoint.sh

# 暴露端口
EXPOSE 6100 6666

# 设置环境变量
ENV FLASK_ENV=production
ENV PYTHONPATH=/app

# 设置健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:6100/api/health || exit 1

# 启动命令
ENTRYPOINT ["/app/entrypoint.sh"]