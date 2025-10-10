# -*- coding:utf-8 -*-
# @FileName  :encrypt_util.py
# @Time      :2025/9/8 19:36
# @Author    :shi lei.wei  <slwei@eppei.com>.
import gzip
import json
from base64 import b64encode, b64decode

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad

from config_manager import ConfigManager


def encrypt_data(plaintext: str) -> str:
    """
    AES加密数据，IV与密文拼接
    压缩 + 加密 + Base64 编码
    返回可传输的字符串
    """
    # 1. UTF-8 编码
    text_bytes = plaintext.encode('utf-8')
    # 2. GZIP 压缩
    compressed = gzip.compress(text_bytes)
    # 3. AES-256-CBC 加密
    # 生成随机IV (AES的IV固定为16字节)
    iv = get_random_bytes(16)
    # 创建加密器
    the_key = ConfigManager.get_init_param_by_key("key", "1d5fd0779a124c5f8ec06bd3282f3a69")
    cipher = AES.new(b64decode(the_key), AES.MODE_CBC, iv)
    # 填充并加密
    padded_data = pad(compressed, AES.block_size)
    encrypted_data = cipher.encrypt(padded_data)
    # 将IV和密文拼接在一起
    # 格式: IV(16字节) + 密文
    combined_data = iv + encrypted_data
    # 5. Base64 编码
    # 返回拼接后的数据（base64编码）
    return b64encode(combined_data).decode('utf-8')


if __name__ == "__main__":
    # === 使用示例：模拟 API 推送 ===
    print(b64encode(get_random_bytes(32)).decode('utf-8'))
    message = "这是一段需要压缩和加密的长文本，用于测试 HTTP API 推送效率和安全性。" * 20
    encrypted_data1 = encrypt_data(message)
    payload = {
        "data": encrypted_data1
    }
    print("发送到 API 的 JSON 数据：")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
