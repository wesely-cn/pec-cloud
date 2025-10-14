# -*- coding:utf-8 -*-
# @FileName  :back_off_queue.py
# @Time      :2025/10/12 17:05
# @Author    :shi lei.wei  <slwei@eppei.com>.

import logging
import random
import threading
import time
from multiprocessing import Queue
from queue import Empty
from typing import Callable, Any, Optional

# 基于PriorityQueue实现的，指数退避重试任务队列，但是多进程不共享数据，所以改为multiprocess.Queue
logger = logging.getLogger(__name__)


class ExponentialBackoffQueue:
    """
    一个支持指数退避重试的优先级队列。
    任务失败后会自动按指数退避时间重新入队，直到成功或达到最大重试次数。
    """

    def __init__(
        self,
        process_func: Callable[[Any], None],
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_backoff: float = 60.0,
        jitter: bool = True,
        dead_letter_callback: Optional[Callable[[Any, int, Exception], None]] = None,
        worker_count: int = 1
    ):
        """
        :param process_func: 处理任务的函数，接受一个参数 data
        :param max_retries: 最大重试次数（包含初始尝试）
        :param base_delay: 基础延迟时间（秒）
        :param max_backoff: 最大退避时间（秒）
        :param jitter: 是否添加随机抖动（推荐开启）
        :param dead_letter_callback: 当任务达到最大重试次数时的回调函数
        :param worker_count: 启动多少个工作线程
        """
        self.process_func = process_func
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_backoff = max_backoff
        self.jitter = jitter
        self.dead_letter_callback = dead_letter_callback
        # 优先级队列: (next_run_time, data, retry_count)，多进程不共享数据！所以在多进程异常
        # self.queue = queue.PriorityQueue()
        self.queue = Queue(maxsize=1024)
        # 启动工作线程
        for i in range(worker_count):
            t = threading.Thread(target=self._worker, name=f"BackoffWorker-{i}", daemon=True)
            t.start()
        # 启动监控线程
        monitor_t = threading.Thread(target=self._stat_queue, name=f"BackoffWorker-Stat", daemon=True)
        monitor_t.start()
        logger.info(f"ExponentialBackoffQueue 启动，{worker_count} 个工作线程，最大重试: {max_retries}")

    def add_task(self, data: Any):
        """
        添加新任务（初始重试次数为 0）
        """
        # 下一次执行时间：立即执行（time.time()）
        self.queue.put((time.time(), data, 0))
        logger.debug(f"📥 添加任务: {data}")

    def _worker(self):
        """
        工作线程：从队列取出任务并处理
        """
        while True:
            try:
                next_time, data, retry_count = self.queue.get(timeout=1)
                # 如果还没到执行时间，放回队列
                now = time.time()
                if now < next_time:
                    self.queue.put((next_time, data, retry_count))
                    time.sleep(60)
                    continue
                # 执行任务
                try:
                    self.process_func(data)
                    logger.debug(f"✅ 成功处理: {data}")
                except Exception as e:
                    retry_count += 1
                    if retry_count < self.max_retries:
                        delay = (2 ** retry_count) * self.base_delay
                        delay = min(delay, self.max_backoff)
                        # 添加抖动
                        if self.jitter:
                            delay += random.uniform(0, 1)
                        next_run_time = now + delay
                        self.queue.put((next_run_time, data, retry_count))
                        logger.warning(f"🔁 {data} 第 {retry_count} 次失败，{delay:.2f}s 后重试")
                    else:
                        logger.error(f"💀 {data} 达到最大重试次数 {self.max_retries}，放弃")
                        if self.dead_letter_callback:
                            self.dead_letter_callback(data, retry_count, e)
                # finally:
                #     self.queue.task_done()
            except Empty:
                continue
            except Exception as e:
                logger.exception(f"Worker 发生未预期错误: {e}")

    # def join(self):
    #     """等待所有任务完成"""
    #     self.queue.join()

    def _stat_queue(self):
        """返回队列中任务数量（近似值）"""
        while True:
            try:
                logger.info("backoff queue size: %d", self.queue.qsize())
            except Exception as e:
                logger.exception(e)
            finally:
                time.sleep(60)


# 定义你的处理函数
def my_process_function(data):
    logger.info(f"🎯 正在处理: {data}")
    if random.random() < 0.8:  # 80% 概率失败
        raise Exception("模拟失败")
    logger.info(f"🎉 成功: {data}")


# 定义死信队列回调
def on_permanent_failure(data, retry_count, exception):
    logger.exception(f"🚨 永久失败: {data}, 重试: {retry_count}, 错误: {exception}")


if __name__ == "__main__":
    # 创建队列
    ebq = ExponentialBackoffQueue(
        process_func=my_process_function,
        max_retries=4,
        base_delay=1.0,
        max_backoff=30.0,
        jitter=True,
        dead_letter_callback=on_permanent_failure,
        worker_count=2  # 两个工作线程
    )
    # 添加任务
    for idd in range(3):
        ebq.add_task(f"消息-{idd}")
    # 保持主线程运行（实际中你可能用其他方式）
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("程序退出")
