# -*- coding:utf-8 -*-
# @FileName  :zeromq_server.py
# @Time      :2025/9/4 15:54
# @Author    :shi lei.wei  <slwei@eppei.com>.
# server.py (外网)
import json
import logging
import threading
import time
import zlib
from multiprocessing import Queue
from queue import Empty

import zmq
from flask import Flask, request, jsonify

from back_off_queue import on_permanent_failure, ExponentialBackoffQueue
from config_manager import load_config, ConfigManager
from log import setup_logger
from login_api import LoginApi

logger = logging.getLogger(__name__)


class DataPublisher:
    def __init__(self, flask_app: Flask, zmq_bind_address="tcp://0.0.0.0:6666", api_port=6100, shared_queue=None):
        self.api_port = api_port
        # ZMQ配置
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.PUB)
        self.zmq_socket.bind(zmq_bind_address)

        # API配置
        # self.app = Flask(__name__)
        self.app = flask_app
        # 缓冲队列
        self.data_queue = shared_queue
        self.running = True
        self.sequence_counter = 0
        self.sequence_lock = threading.Lock()

        # 注册API路由
        # self._register_routes()

        # 启动数据发布线程
        self.publish_thread = threading.Thread(target=self._publish_data_loop, daemon=True)
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeat, daemon=True)
        self.start(zmq_bind_address)
        # 启动重试线程
        self.ebq = ExponentialBackoffQueue(
            process_func=self._process_data,
            max_retries=5,
            base_delay=60.0,
            max_backoff=60 * 60 * 6.0,
            jitter=True,
            dead_letter_callback=on_permanent_failure,
            worker_count=2
        )

    def _register_routes(self):
        """注册API路由"""

        @self.app.route('/api/data2', methods=['POST'])
        def receive_data():
            try:
                # 接收JSON数据
                data = request.get_json()
                if not data:
                    return jsonify({"error": "No JSON data provided"}), 400
                # 验证必要字段
                if 'data' not in data:
                    return jsonify({"error": "Missing 'data' field"}), 400
                # 添加到队列（阻塞等待，直到有空间）
                queue_data = {
                    "payload": data,
                    "received_at": time.time()
                }
                # # 改为接受纯文本
                # text_data = request.get_data(as_text=True)
                # 阻塞put
                self.data_queue.put(queue_data)
                logger.info(f"数据已接收并加入队列，队列大小: {self.data_queue.qsize()}")
                return jsonify({"status": "success", "message": "Data received"}), 200
            except Exception as e:
                logger.error(f"接收数据错误: {e}")
                return jsonify({"error": str(e)}), 500

    def compress_data(self, data):
        """压缩数据"""
        json_data = json.dumps(data)
        compressed = zlib.compress(json_data.encode('utf-8'))
        return compressed

    def _send_heartbeat(self):
        """心跳线程"""
        while self.running:
            try:
                heartbeat_data = {
                    "type": "heartbeat",
                    "timestamp": time.time(),
                    "status": "alive",
                    "queue_size": self.data_queue.qsize()
                }
                compressed_heartbeat = self.compress_data(heartbeat_data)
                self.zmq_socket.send_multipart([b"heartbeat", compressed_heartbeat])
                logger.debug(f"[心跳] 发送心跳包")
                time.sleep(60)
            except Exception as e:
                logger.error(f"[心跳] 错误: {e}")
                time.sleep(30)

    def _get_next_sequence(self):
        """获取下一个序列号"""
        with self.sequence_lock:
            seq = self.sequence_counter
            self.sequence_counter += 1
            return seq

    def _publish_data_loop(self):
        """数据发布循环 - 使用阻塞队列读取"""
        logger.info("数据发布循环启动")
        while self.running:
            queue_data = None
            try:
                # 阻塞等待队列数据（超时1秒，避免无法响应停止信号）
                queue_data = self.data_queue.get(timeout=1)
                # 获取序列号
                # sequence = self._get_next_sequence()
                # 构造完整数据包
                # full_data = {
                #     "type": "data",
                #     "sequence": sequence,
                #     "payload": queue_data["payload"],
                #     "received_at": queue_data["received_at"],
                #     "published_at": time.time()
                # }
                # 压缩并发送
                # compressed_data = self.compress_data(full_data)
                # encrypt_data = encrypt_util.encrypt_data(json.dumps(full_data))
                # 这里采集端上传的时候已经压缩过了，所以直接传
                logger.info("zmq push data: %s", str(queue_data["received_at"]))
                self.zmq_socket.send_multipart([b"data", queue_data["payload"].encode("utf-8")])

                # original_size = len(json.dumps(full_data).encode('utf-8'))
                # compressed_size = len(encrypt_data)
                # compression_ratio = (1 - compressed_size / original_size) * 100
                #
                # logger.info(f"[数据] 发送消息 #{sequence}")
                # logger.info(f"      原始大小: {original_size} 字节")
                # logger.info(f"      压缩后: {compressed_size} 字节")
                # logger.info(f"      压缩率: {compression_ratio:.2f}%")
                # 标记任务完成
                # self.data_queue.task_done()
            except Empty:
                # 超时，继续循环检查running状态
                continue
            except Exception as e:
                logger.exception(f"发布数据异常: {e}")
                if queue_data:
                    self.ebq.add_task(queue_data)
                time.sleep(1)

    def _process_data(self, queue_data):
        # 这里采集端上传的时候已经压缩过了，所以直接传
        logger.info("zmq re-push data: %s", str(queue_data["received_at"]))
        self.zmq_socket.send_multipart([b"data", queue_data["payload"].encode("utf-8")])

    def add_data(self, data):
        """添加数据到队列"""
        try:
            queue_data = {
                "payload": data,
                "received_at": time.time()
            }
            self.data_queue.put(queue_data, timeout=5)
            return True
        except Exception as e:
            logger.exception("队列已满，数据添加失败", e)
            return False

    def stop(self):
        """停止服务"""
        self.running = False
        self.publish_thread.join(timeout=5)
        self.heartbeat_thread.join(timeout=5)
        self.zmq_socket.close()
        self.zmq_context.term()

    def start(self, zmq_bind_address="tcp://0.0.0.0:6666"):
        """启动服务"""
        # 启动线程
        self.publish_thread.start()
        # 是否需要发送心跳？占用流量
        self.heartbeat_thread.start()

        logger.info("数据发布服务启动完成")
        logger.info("ZMQ服务: %s", zmq_bind_address)
        logger.info("API服务: http://0.0.0.0:6100")


# 创建Flask应用
def create_app(api_port, zmq_bind_address):
    app = Flask(__name__)
    # 在主进程创建队列
    shared_queue = Queue(maxsize=1000)
    # 传递给 worker 进程（需在 fork 前设置好）
    # 创建全局DataPublisher实例
    app.publisher = DataPublisher(app, zmq_bind_address, api_port, shared_queue)
    # 创建LoginApi实例，账号登录状态接口
    LoginApi(app)

    @app.route('/api/data', methods=['POST'])
    def receive_data():
        try:
            # 改为接受纯文本
            text_data = request.get_data(as_text=True)
            if app.publisher.add_data(text_data):
                logger.info(f"数据已接收并加入队列，队列大小: {app.publisher.data_queue.qsize()}")
                return jsonify({"status": "success", "message": "Data received"}), 200
            else:
                return jsonify({"error": "Queue full, try again later"}), 503
        except Exception as e:
            logger.error(f"接收数据错误: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route('/api/batch_data', methods=['POST'])
    def receive_batch_data():
        """批量接收数据接口"""
        try:
            data_list = request.get_json()
            if not isinstance(data_list, list):
                return jsonify({"error": "Expected JSON array"}), 400
            success_count = 0
            for data in data_list:
                app.publisher.add_data(data)
                success_count += 1
            logger.info(f"批量数据接收完成: {success_count} 条")
            return jsonify({
                "status": "success",
                "message": f"Received {success_count} data items"
            }), 200
        except Exception as e:
            logger.error(f"批量接收数据错误: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route('/api/health', methods=['GET'])
    def health_check():
        return jsonify({
            "status": "healthy",
            "queue_size": app.publisher.data_queue.qsize(),
            "timestamp": time.time()
        }), 200

    @app.route('/api/stats', methods=['GET'])
    def get_stats():
        """获取统计信息"""
        return jsonify({
            "queue_size": app.publisher.data_queue.qsize(),
            "max_queue_size": app.publisher.data_queue.maxsize,
            "zmq_address": "tcp://0.0.0.0:5555"
        }), 200

    @app.teardown_appcontext
    def cleanup_publisher(exception):
        pass

    return app


load_config()
setup_logger()
c_port = ConfigManager.get_param_by_key("api_port", 6100)
zmq_address = ConfigManager.get_param_by_key("zmq_address", "tcp://0.0.0.0:6666")
debug_mode = ConfigManager.get_param_by_key("debug_mode", False)
# 用于Gunicorn启动
gun_app = create_app(c_port, zmq_address)

if __name__ == "__main__":
    # 直接运行时使用Flask开发服务器（仅用于开发测试）
    # gun_app.run(host='0.0.0.0', port=c_port, threaded=True, debug=debug_mode)
    # # 创建全局实例
    # publisher = DataPublisher(zmq_address, c_port)
    # try:
    #     publisher.start()
    # except KeyboardInterrupt:
    #     logger.info("服务停止...")
    #     publisher.stop()
    pass
