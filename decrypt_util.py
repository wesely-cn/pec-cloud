# -*- coding:utf-8 -*-
# @FileName  :decrypt_util.py
# @Time      :2025/9/8 19:22
# @Author    :shi lei.wei  <slwei@eppei.com>.
import gzip
import logging
from base64 import b64decode

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

from config_manager import ConfigManager

logger = logging.getLogger(__name__)


def decrypt_data(encrypted_b64: str) -> str | None:
    """
    解密数据（从拼接的数据中提取IV和密文）
    从 Base64 数据中解密并解压
    返回原始文本
    """
    try:
        # 1. Base64 解码，解码拼接的数据
        combined_data = b64decode(encrypted_b64)
        # 2. 提取IV和密文（IV固定16字节）
        iv = combined_data[:16]  # 前16字节是IV
        encrypted_data = combined_data[16:]  # 剩余部分是密文
        logger.debug(f"提取IV长度: {len(iv)} 字节")
        logger.debug(f"提取密文长度: {len(encrypted_data)} 字节")
        # 3. 创建解密器
        the_key = ConfigManager.get_init_param_by_key("key", "1d5fd0779a124c5f8ec06bd3282f3a69")
        cipher = AES.new(b64decode(the_key), AES.MODE_CBC, iv)
        # 解密并去除填充
        decrypted_data = unpad(cipher.decrypt(encrypted_data), AES.block_size)
        # 4. GZIP 解压
        text_bytes = gzip.decompress(decrypted_data)
        # 5. UTF-8 解码
        return text_bytes.decode('utf-8')
    except Exception as e:
        logger.exception(f"解密错误: {e}")
        return None


if __name__ == "__main__":
    # === 模拟接收 API 请求 ===
    encrypted_data1 = "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"
    try:
        decrypted_text1 = decrypt_data(encrypted_data1)
        print("\n解密解压成功，内容为：")
        print(decrypted_text1[:100] + "...")
    except Exception as e1:
        print(f"解密失败: {e1}")
