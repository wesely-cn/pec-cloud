# -*- coding:utf-8 -*-
# @FileName  :zeromq-client.py
# @Time      :2025/9/4 15:55
# @Author    :shi lei.wei  <slwei@eppei.com>.
# client.py (内网)
import logging

import zmq
import json
import zlib
import time
import threading
from datetime import datetime

import decrypt_util
from action_util import call_third_api
from config_manager import load_config, ConfigManager
from log import setup_logger

logger = logging.getLogger(__name__)


class DataSubscriber:
    def __init__(self, server_address="tcp://0.0.0.0:6666", recv_timeout=10000):
        """
        初始化订阅者
        :param server_address: 服务端地址
        :param recv_timeout: 接收超时时间（毫秒），None表示永久阻塞
        """
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.server_address = server_address
        self.socket.connect(server_address)
        # 订阅所有消息
        self.socket.setsockopt(zmq.SUBSCRIBE, b"")
        # 设置接收超时
        if recv_timeout is not None:
            self.socket.setsockopt(zmq.RCVTIMEO, recv_timeout)
        self.running = True
        self.last_heartbeat = time.time()
        # 30秒无心跳认为连接异常
        self.heartbeat_timeout = 180
        logger.info("zero mq client bind address: %s", server_address)

    def decompress_data(self, compressed_data):
        """解压数据"""
        try:
            decompressed = zlib.decompress(compressed_data)
            return json.loads(decompressed.decode('utf-8'))
        except Exception as e:
            logger.error(f"解压错误: {e}")
            logger.exception(e)
            return None

    def monitor_heartbeat(self):
        """心跳监控线程"""
        while self.running:
            current_time = time.time()
            if current_time - self.last_heartbeat > self.heartbeat_timeout:
                logger.warning(f"[警告] 心跳超时！最后心跳时间: {datetime.fromtimestamp(self.last_heartbeat)}")
                # 可以在这里添加重连逻辑
                self.socket.connect(self.server_address)
                # 订阅所有消息
                self.socket.setsockopt(zmq.SUBSCRIBE, b"")
            time.sleep(5)

    def start_subscribing(self):
        """开始订阅数据"""
        # 启动心跳监控线程
        monitor_thread = threading.Thread(target=self.monitor_heartbeat, daemon=True)
        monitor_thread.start()
        logger.info("内网客户端启动，等待接收数据...")
        try:
            while self.running:
                try:
                    # 接收多部分消息
                    message_parts = self.socket.recv_multipart()
                    if len(message_parts) >= 2:
                        msg_type = message_parts[0]
                        compressed_data = message_parts[1]
                        if msg_type == b"heartbeat":
                            # 处理心跳包
                            heartbeat_data = self.decompress_data(compressed_data)
                            if heartbeat_data:
                                self.last_heartbeat = time.time()
                                logger.info(f"[心跳] 收到心跳包 - {datetime.fromtimestamp(heartbeat_data['timestamp'])} - "
                                            f"{heartbeat_data['queue_size']}")
                        elif msg_type == b"data":
                            # 处理数据包
                            start_time = time.time()
                            # data = self.decompress_data(compressed_data)
                            data = decrypt_util.decrypt_data(compressed_data.decode("utf-8"))
                            if data:
                                process_time = time.time() - start_time
                                self.process_data(json.loads(data), process_time)
                            else:
                                logger.info("数据解压失败")
                        else:
                            logger.info(f"未知消息类型: {msg_type}")
                    else:
                        logger.info("接收到不完整的消息")
                except zmq.Again:
                    # 超时，继续循环
                    continue
                except Exception as e:
                    logger.error(f"接收错误: {e}")
                    logger.exception(e)
                    time.sleep(10)
        except KeyboardInterrupt:
            logger.error("客户端停止...")
        finally:
            self.stop()

    def process_data(self, data, process_time):
        """处理接收到的数据"""
        try:
            sequence = data.get('sequence', 'N/A')
            timestamp = data.get('timestamp', 'N/A')
            data_size = len(data.get('data', ''))
            logger.info(f"[数据] 接收消息 #{sequence}")
            logger.info(
                f"      时间戳: {datetime.fromtimestamp(timestamp) if isinstance(timestamp, (int, float)) else timestamp}")
            logger.info(f"      数据大小: {data_size} 字节")
            logger.info(f"      处理耗时: {process_time:.3f} 秒")
            # 转发到辅助决策系统
            call_third_api(data.get('data'))
        except Exception as e:
            logger.error(f"数据处理错误: {e}")
            logger.exception(e)

    def stop(self):
        """停止客户端"""
        self.running = False
        self.socket.close()
        self.context.term()


if __name__ == "__main__":
    load_config(filename="config_client.json")
    setup_logger()
    zmq_address = ConfigManager.get_param_by_key("zmq_address", "tcp://101.201.53.86:6666")
    subscriber = DataSubscriber(zmq_address)
    subscriber.start_subscribing()
