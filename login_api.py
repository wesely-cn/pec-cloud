# -*- coding:utf-8 -*-
# @FileName  :login_api.py
# @Time      :2025/8/28 21:32
# @Author    :shi lei.wei  <slwei@eppei.com>.
# 可选：在应用关闭时清理资源
import hashlib
import logging
import random
import sqlite3
import threading
import time
from contextlib import contextmanager
# 推荐使用 queue 来管理线程池
from multiprocessing import Queue
from queue import Empty

# app.py
from flask import Flask, request, jsonify

from config_manager import ConfigManager, load_config

logger = logging.getLogger(__name__)
# --- 配置 ---
DB_NAME = 'login_status.db'
# 线程池大小
THREAD_POOL_SIZE = 4
active_unit = {}
# API_KEY
API_KEYS = {
    "unit-epm-pm-epise-chd-gs-pec": "1974596b2bb3423e8ec16ea0851455a8"
}


# --- 数据库初始化 ---
def init_db():
    """初始化数据库，创建表（如果不存在）"""
    # 使用上下文管理器确保连接关闭
    with get_db_connection() as conn:
        c = conn.cursor()
        # 使用 IF NOT EXISTS 确保幂等性
        c.execute('''
            CREATE TABLE IF NOT EXISTS logins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                unit TEXT NOT NULL,
                unit_id INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                machine TEXT NOT NULL,
                state INTEGER,
                ip TEXT -- 客户端IP
            )
        ''')
        conn.commit()
        logger.info("数据库已初始化或已存在。")


# --- 数据库连接管理 ---
# 使用 threading.local() 为每个线程创建独立的连接
# 这在单个工作进程（如Flask开发服务器或单个Gunicorn worker）中是安全的
# 对于多进程部署，每个进程仍会有自己的 local_storage
local_storage = threading.local()


@contextmanager
def get_db_connection():
    """获取数据库连接的上下文管理器，确保连接被正确关闭"""
    # 尝试获取当前线程的连接
    if not hasattr(local_storage, 'connection'):
        # 如果当前线程没有连接，则创建一个新的
        local_storage.connection = sqlite3.connect(DB_NAME, check_same_thread=False)
        # 可选：设置行工厂以方便访问列
        local_storage.connection.row_factory = sqlite3.Row
        logger.info(f"线程 {threading.get_ident()} 创建了新的数据库连接。")

    try:
        yield local_storage.connection
    except Exception as e:
        local_storage.connection.rollback()
        raise e
    else:
        # 注意：在使用线程本地连接时，通常不在这里提交，
        # 提交应该在具体的业务逻辑中根据需要进行。
        # 这里主要是为了确保异常时回滚。
        pass
    # finally:
    #     # 不要在这里关闭连接，因为我们希望在同一线程内复用它
    #     # 连接会在应用关闭或线程结束时（由 Python 垃圾回收）处理
    #     # 如果需要主动关闭，应该提供一个单独的关闭函数


def close_thread_db_connection():
    """关闭当前线程的数据库连接（可选，用于清理）"""
    if hasattr(local_storage, 'connection'):
        try:
            local_storage.connection.close()
            logger.info(f"线程 {threading.get_ident()} 关闭了数据库连接。")
        except Exception as e:
            logger.exception(e)
        finally:
            delattr(local_storage, 'connection')


# --- 任务池实现 ---
class TaskPool:
    """一个简单的线程池实现"""

    def __init__(self, num_threads):
        self.tasks = Queue()
        self.threads = []
        self.running = True
        for _ in range(num_threads):
            t = threading.Thread(target=self.worker)
            # 主线程结束时，守护线程也会结束
            t.daemon = True
            t.start()
            self.threads.append(t)

    def worker(self):
        """工作线程函数"""
        while self.running:
            try:
                # 从队列中获取任务，设置超时以便能响应 self.running 状态
                func, args, kwargs = self.tasks.get(timeout=1)
                try:
                    func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"线程池任务执行出错: {e}")
                    logger.exception(e)
                # finally:
                #     self.tasks.task_done()
            except Empty:
                # 超时，继续检查 self.running
                continue

    def add_task(self, func, *args, **kwargs):
        """向线程池添加任务"""
        if self.running:
            self.tasks.put((func, args, kwargs))
            logger.info("add task success!")

    # def wait_completion(self):
    #     """等待所有任务完成"""
    #     self.tasks.join()

    def shutdown(self):
        """关闭线程池"""
        logger.info("关闭线程池")
        self.running = False
        # 等待所有线程结束
        for t in self.threads:
            t.join()


# 全局线程池实例
task_pool = TaskPool(THREAD_POOL_SIZE)


# --- 数据库操作函数 (在后台线程中执行) ---
def _record_login_to_db(unit, unit_id, timestamp, machine, state, ip):
    """实际执行数据库写入的操作"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute(
                "INSERT INTO logins (unit, unit_id, timestamp, machine, state, ip) VALUES (?, ?, ?, ?, ?, ?)",
                (unit, unit_id, timestamp, machine, state, ip)
            )
            conn.commit()
            logger.info(f"用户 {unit} 登录状态已记录到数据库 (线程: {threading.get_ident()})")
    except Exception as e:
        logger.error(f"记录登录状态到数据库时出错: {e}")
        logger.exception(e)
    # finally:
    #     # 可选：任务完成后关闭线程的数据库连接
    #     close_thread_db_connection()


def _get_status_from_db(limit=10):
    """实际执行数据库查询的操作 (供后台线程使用，如果需要)"""
    # 此示例中，查询直接在主线程（处理HTTP请求的线程）中进行，
    # 因为查询通常很快。但如果查询很重，也可以放入线程池。
    pass  # 占位符


# --- Flask 应用 ---
class LoginApi:
    def __init__(self, app: Flask):
        self.app = app
        self.unit_pool = ConfigManager.get_init_param_by_key("unit_pool")
        self.unit_name = ConfigManager.get_init_param_by_key("unit_name")
        self.init_db()
        self._register_routes()
        # atexit.register(self.cleanup)

    def init_db(self):
        # 应用启动时初始化数据库
        with self.app.app_context():
            init_db()

    def _register_routes(self):
        logger.info("register login api")

        @self.app.route('/api/login_status', methods=['POST'])
        def record_login():
            """接收登录状态并提交给线程池处理"""
            data = request.get_json()
            if not data:
                return jsonify({"error": "无效的JSON数据"}), 400
            logger.info("received: %s", data)
            unit = data.get('unitName')
            timestamp = data.get('timestamp')
            machine = data.get('uniqueId', 'Unknown')
            # 获取客户端IP
            # ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
            ip = data.get('ip')
            if unit:
                unit_id = self.unit_name.get(unit)
                if not unit_id:
                    logger.error("unknown unit: %s", unit)
                    return jsonify({"status": "unknown unit, no content"}), 204
                # 记录活跃的账号
                active_unit.setdefault(machine, unit_id)
                # 将写入数据库的任务提交到线程池，避免阻塞HTTP响应
                task_pool.add_task(_record_login_to_db, unit, unit_id, timestamp, machine, 1, ip)
                # 202 Accepted
                return jsonify({"status": "received and queued"}), 202
            else:
                return jsonify({"error": "缺少用户信息"}), 400

        @self.app.route('/api/login_status', methods=['GET'])
        def get_status():
            """获取登录状态 (直接在请求线程中执行查询)"""
            try:
                # 直接在处理HTTP请求的线程中执行查询
                # 如果查询非常耗时，可以考虑也放入线程池
                limit = request.args.get('limit', 10, type=int)
                unit = request.args.get('unit', type=str)
                with get_db_connection() as conn:
                    c = conn.cursor()
                    if unit:
                        query_sql = ("SELECT id, unit, unit_id, timestamp, machine, state, ip FROM logins "
                                     "WHERE unit=? ORDER BY timestamp DESC LIMIT ?")
                        c.execute(query_sql, (unit, limit))
                    else:
                        query_sql = ("SELECT id, unit, unit_id, timestamp, machine, state, ip FROM logins "
                                     "ORDER BY timestamp DESC LIMIT ?")
                        c.execute(query_sql, (limit,))
                    rows = c.fetchall()
                # 使用 sqlite3.Row 可以像字典一样访问列
                logins = [dict(row) for row in rows]
                return jsonify(logins)
            except Exception as e:
                logger.error(f"查询数据库时出错: {e}")
                logger.exception(e)
                return jsonify({"error": "内部服务器错误"}), 500

        @self.app.route('/api/available_unit', methods=['GET'])
        def get_available_unit():
            """获取可登录的账号"""
            try:
                # 获取请求头
                api_key = request.headers.get('X-API-Key')
                timestamp = request.headers.get('X-Timestamp')
                signature = request.headers.get('X-Signature')
                # 验证签名
                if not all([api_key, timestamp, signature]):
                    return jsonify({'error': 'Missing authentication headers'}), 401

                if not self.verify_signature(api_key, timestamp, signature):
                    return jsonify({'error': 'Invalid signature'}), 401
                # 返回非活跃的账号
                active_units = active_unit.values()
                if not active_units:
                    active_units = self.get_today_unit()
                return self.get_random_name_by_priority(list(active_units))
            except Exception as e:
                logger.error(f"查询数据库时出错: {e}")
                logger.exception(e)
                return jsonify({"error": "内部服务器错误"}), 500

        @self.app.route('/api/logout', methods=['POST'])
        def unit_logout():
            """账号已登出"""
            try:
                # 移除活跃的账号
                data = request.get_json()
                if not data:
                    return jsonify({"error": "无效的JSON数据"}), 400
                unit = data.get('unitName')
                timestamp = data.get('timestamp')
                machine = data.get('uniqueId', 'Unknown')
                if machine in active_unit:
                    unit_id = self.unit_name.get(unit)
                    active_unit.pop(machine)
                    ip = data.get('ip')
                    task_pool.add_task(_record_login_to_db, unit, unit_id, timestamp, machine, 0, ip)
                    logger.info("unit logout: %s, %s", unit, timestamp)
                else:
                    logger.warning("unit not active: %s, %s, %s", unit, timestamp, machine)
                return jsonify({"status": "success"}), 200
            except Exception as e:
                logger.error(f"查询数据库时出错: {e}")
                logger.exception(e)
                return jsonify({"error": "内部服务器错误"}), 500

    def cleanup(self):
        logger.info("正在关闭应用...")
        task_pool.shutdown()
        # 关闭所有线程的数据库连接
        # 这在守护线程和应用退出时可能不是必须的，但作为示例
        # 可以遍历所有活动线程并调用 close_thread_db_connection
        # 但在简单场景下，Python的垃圾回收通常会处理这些。

    def get_random_name_by_priority(self, active_name):
        """
        从 name_pool 中随机选择一个不在 active_name 列表中的 key，
        选择时根据 value（优先级）进行加权。

        Args:
            # name_pool (dict): 名称和优先级的字典，如 {"name1": 23, "name2": 30}
            active_name (list): 当前活跃的名称列表

        Returns:
            str or None: 选中的名称，如果没有符合条件的返回 None
        """
        # 过滤出不在 active_name 中的候选 key
        candidates = [name for name in self.unit_pool.keys() if name not in active_name]
        # 如果没有候选者，返回 None
        if not candidates:
            return None
        # 提取对应候选者的优先级（权重）
        weights = [1 / self.unit_pool[name] for name in candidates]
        # 使用 random.choices 进行加权随机选择（weights 参数）
        # k=1 表示返回一个元素，结果是列表，取第一个
        chosen = random.choices(candidates, weights=weights, k=1)[0]
        return chosen

    def verify_signature(self, api_key, timestamp, signature):
        """验证签名"""
        if api_key not in API_KEYS:
            return False
        # 检查时间戳（防止重放攻击，允许5分钟偏差）
        current_time = int(time.time())
        if abs(current_time - int(timestamp)) > 300:
            return False
        secret_key = API_KEYS[api_key]
        sign_string = f"{api_key}{timestamp}{secret_key}"
        expected_signature = hashlib.sha256(sign_string.encode()).hexdigest()
        return signature == expected_signature

    def get_today_unit(self):
        with get_db_connection() as conn:
            c = conn.cursor()
            query_sql = "SELECT unit_id FROM logins WHERE date(timestamp) = date('now') AND state=1"
            c.execute(query_sql)
            rows = c.fetchall()
        return [row[0] for row in rows]


# --- 测试应用入口 ---
if __name__ == '__main__':
    # --- 示例使用 ---
    load_config()
    flask_app = Flask(__name__)
    login_api = LoginApi(flask_app)
    logger.info("Flask 应用已启动，数据库已初始化。")
    name_pool1 = {"name1": 23, "name2": 30, "name3": 88}
    active_name1 = ["name1", "name99"]
    for _ in range(10):
        selected = login_api.get_random_name_by_priority(active_name1)
        print(selected)
    # 启动 Flask 开发服务器
    # 生产环境应关闭 debug
    flask_app.run(host='0.0.0.0', port=5000, debug=False)
