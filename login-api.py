# -*- coding:utf-8 -*-
# @FileName  :login-api.py
# @Time      :2025/8/28 21:32
# @Author    :shi lei.wei  <slwei@eppei.com>.
# 可选：在应用关闭时清理资源
import atexit
import logging
# 推荐使用 queue 来管理线程池
import queue
import sqlite3
import threading
from contextlib import contextmanager

# app.py
from flask import Flask, request, jsonify

logger = logging.getLogger(__name__)
# --- 配置 ---
DB_NAME = 'login_status.db'
# 线程池大小
THREAD_POOL_SIZE = 4


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
                user TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                machine TEXT,
                ip TEXT -- 可选：记录客户端IP
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
        except:
            pass
        finally:
            delattr(local_storage, 'connection')


# --- 线程池实现 ---
class ThreadPool:
    """一个简单的线程池实现"""

    def __init__(self, num_threads):
        self.tasks = queue.Queue()
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
                finally:
                    self.tasks.task_done()
            except queue.Empty:
                # 超时，继续检查 self.running
                continue

    def add_task(self, func, *args, **kwargs):
        """向线程池添加任务"""
        if self.running:
            self.tasks.put((func, args, kwargs))

    def wait_completion(self):
        """等待所有任务完成"""
        self.tasks.join()

    def shutdown(self):
        """关闭线程池"""
        self.running = False
        # 等待所有线程结束
        for t in self.threads:
            t.join()


# 全局线程池实例
task_pool = ThreadPool(THREAD_POOL_SIZE)


# --- 数据库操作函数 (在后台线程中执行) ---
def _record_login_to_db(user, timestamp, machine, ip):
    """实际执行数据库写入的操作"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute(
                "INSERT INTO logins (user, timestamp, machine, ip) VALUES (?, ?, ?, ?)",
                (user, timestamp, machine, ip)
            )
            conn.commit()
            logger.info(f"用户 {user} 登录状态已记录到数据库 (线程: {threading.get_ident()})")
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
        self.init_db()
        self._register_routes()

    def init_db(self):
        # 应用启动时初始化数据库
        with self.app.app_context():
            init_db()

    def _register_routes(self):
        @self.app.route('/api/login_status', methods=['POST'])
        def record_login():
            """接收登录状态并提交给线程池处理"""
            data = request.get_json()
            if not data:
                return jsonify({"error": "无效的JSON数据"}), 400

            user = data.get('user')
            timestamp = data.get('timestamp')
            machine = data.get('machine', 'Unknown')
            # 获取客户端IP
            ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

            if user:  # 简单验证
                # 将写入数据库的任务提交到线程池，避免阻塞HTTP响应
                task_pool.add_task(_record_login_to_db, user, timestamp, machine, ip)
                return jsonify({"status": "received and queued"}), 202  # 202 Accepted
            else:
                return jsonify({"error": "缺少用户信息"}), 400

        @self.app.route('/api/login_status', methods=['GET'])
        def get_status():
            """获取登录状态 (直接在请求线程中执行查询)"""
            try:
                # 直接在处理HTTP请求的线程中执行查询
                # 如果查询非常耗时，可以考虑也放入线程池
                limit = request.args.get('limit', 10, type=int)
                account = request.args.get('account', '1', type=str)

                with get_db_connection() as conn:
                    c = conn.cursor()
                    c.execute(
                        "SELECT id, user, timestamp, machine, ip FROM logins WHERE user=? "
                        "ORDER BY timestamp DESC LIMIT ?",
                        (account, limit)  # 假设只查询用户 'b'
                    )
                    rows = c.fetchall()

                # 使用 sqlite3.Row 可以像字典一样访问列
                logins = [dict(row) for row in rows]
                return jsonify(logins)
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

    atexit.register(cleanup)


# --- 测试应用入口 ---
if __name__ == '__main__':
    flask_app = Flask(__name__)
    login_api = LoginApi(flask_app)
    logger.info("Flask 应用已启动，数据库已初始化。")
    # 启动 Flask 开发服务器
    # 生产环境应关闭 debug
    flask_app.run(host='0.0.0.0', port=5000, debug=False)
