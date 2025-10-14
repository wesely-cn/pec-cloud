# -*- coding:utf-8 -*-
# @FileName  :back_off_ap_queue.py
# @Time      :2025/10/12 16:51
# @Author    :shi lei.wei  <slwei@eppei.com>.

import random
import threading
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from typing import Any, Callable, Dict, Optional
import logging

logger = logging.getLogger(__name__)


# 基于APscheduler实现的，指数退避重试任务队列，暂未使用
class ExponentialBackoffQueue:
    """
    一个支持指数退避重试的任务队列，基于 APScheduler 实现。
    """

    def __init__(
        self,
        task_func: Callable[[Any], None],
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_backoff: float = 60.0,
        jitter: bool = True,
        job_store: str = 'memory'
    ):
        """
        :param task_func: 处理任务的函数，接受一个参数 data
        :param max_retries: 最大重试次数
        :param base_delay: 基础延迟时间（秒）
        :param max_backoff: 最大退避时间（秒）
        :param jitter: 是否启用随机抖动（推荐开启）
        :param job_store: 存储方式，'memory' 或 'redis' 等（可扩展）
        """
        self.task_func = task_func
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_backoff = max_backoff
        self.jitter = jitter

        # 使用内存存储（适合单机）
        jobstores = {
            'default': MemoryJobStore()
        }

        # 创建后台调度器
        self.scheduler = BackgroundScheduler(jobstores=jobstores)
        self.scheduler.start()

        # 用锁保护共享状态（虽然 APScheduler 是线程安全的）
        self._lock = threading.Lock()

        logger.info(f"✅ ExponentialBackoffQueue 初始化完成，最大重试: {max_retries}")

    def _retry_wrapper(self, data: Any, retry_count: int):
        """包装函数，用于处理重试逻辑"""
        try:
            logger.info(f"🔄 执行任务: {data} (第 {retry_count + 1} 次)")
            self.task_func(data)
            logger.info(f"✅ 成功处理: {data}")
        except Exception as e:
            retry_count += 1
            if retry_count < self.max_retries:
                # 计算指数退避时间
                delay = (2 ** retry_count) * self.base_delay
                delay = min(delay, self.max_backoff)

                # 加上随机抖动
                if self.jitter:
                    delay += random.uniform(0, 1)

                # 下次执行时间
                next_run_time = datetime.now() + timedelta(seconds=delay)

                # 重新添加任务（带重试次数）
                job_id = f"retry_{hash(data)}_{retry_count}"
                self.scheduler.add_job(
                    func=self._retry_wrapper,
                    args=[data, retry_count],
                    trigger='date',
                    run_date=next_run_time,
                    id=job_id,
                    replace_existing=True  # 避免重复
                )
                logger.warning(f"🔁 任务失败，{delay:.2f}s 后重试: {data} (第 {retry_count} 次)")
            else:
                logger.error(f"💀 达到最大重试次数，放弃任务: {data}")
                # 可选：调用 on_failure 回调
                self._on_failure(data, e)

    def _on_failure(self, data: Any, exception: Exception):
        """任务永久失败时的回调（可重写）"""
        # 可扩展：发送告警、存入死信队列等
        pass

    def add_task(self, data: Any, job_id: Optional[str] = None):
        """
        添加任务到队列
        :param data: 任务数据
        :param job_id: 可选的唯一任务ID，用于去重
        """
        job_id = job_id or f"task_{hash(data)}"
        try:
            self.scheduler.add_job(
                func=self._retry_wrapper,
                args=[data, 0],  # 初始重试次数为 0
                trigger='date',
                run_date=datetime.now(),  # 立即执行
                id=job_id,
                replace_existing=True  # 如果已存在，替换
            )
            logger.info(f"📥 添加任务: {data}")
        except Exception as e:
            if "conflicts" in str(e):
                logger.warning(f"⚠️ 任务已存在，跳过: {data}")
            else:
                logger.error(f"❌ 添加任务失败: {e}")

    def shutdown(self, wait=True):
        """关闭调度器"""
        self.scheduler.shutdown(wait=wait)
        logger.info("🛑 ExponentialBackoffQueue 已关闭")

    def print_jobs(self):
        """打印当前所有任务（用于调试）"""
        jobs = self.scheduler.get_jobs()
        for job in jobs:
            print(f"Job: {job.id} | Next Run: {job.next_run_time} | Args: {job.args}")