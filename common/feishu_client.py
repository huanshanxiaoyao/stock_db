#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书机器人客户端模块

提供飞书Webhook机器人的核心功能：
- 文本消息发送
- 富文本消息发送
- 卡片消息发送

作者: Assistant
创建时间: 2025-01-19
更新时间: 2025-01-19
"""

import requests
import json
import time
import hashlib
import hmac
import base64
from typing import Dict, Any, Optional, List


class FeishuWebhookBot:
    """
    飞书Webhook机器人客户端
    
    支持发送文本、富文本和卡片消息到飞书群聊
    """
    
    def __init__(self, webhook_url: str, secret: Optional[str] = None):
        """
        初始化飞书机器人客户端
        
        Args:
            webhook_url: 飞书机器人的Webhook URL
            secret: 机器人的签名密钥（可选）
        """
        self.webhook_url = webhook_url
        self.secret = secret
    
    def _generate_sign(self, timestamp: str) -> str:
        """
        生成签名
        
        Args:
            timestamp: 时间戳
            
        Returns:
            签名字符串
        """
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256
        ).digest()
        sign = base64.b64encode(hmac_code).decode('utf-8')
        return sign
    
    def _send_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        发送消息到飞书
        
        Args:
            payload: 消息载荷
            
        Returns:
            响应结果
        """
        headers = {
            'Content-Type': 'application/json'
        }
        
        # 如果设置了签名密钥，添加签名
        if self.secret:
            timestamp = str(int(time.time()))
            sign = self._generate_sign(timestamp)
            payload['timestamp'] = timestamp
            payload['sign'] = sign
        
        try:
            response = requests.post(
                self.webhook_url,
                headers=headers,
                data=json.dumps(payload),
                timeout=10,
                proxies={'http': None, 'https': None}  # 禁用代理
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {
                'code': -1,
                'msg': f'请求失败: {str(e)}'
            }
    
    def send_text(self, text: str) -> Dict[str, Any]:
        """
        发送文本消息
        
        Args:
            text: 消息内容
            
        Returns:
            发送结果
        """
        payload = {
            "msg_type": "text",
            "content": {
                "text": text
            }
        }
        return self._send_message(payload)
    
    def send_rich_text(self, content: List[List[Dict[str, Any]]], title: str = "富文本消息") -> Dict[str, Any]:
        """
        发送富文本消息
        
        Args:
            content: 富文本内容
            title: 消息标题
            
        Returns:
            发送结果
        """
        payload = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": content
                    }
                }
            }
        }
        return self._send_message(payload)
    
    def send_card(self, card_content: Dict[str, Any]) -> Dict[str, Any]:
        """
        发送卡片消息
        
        Args:
            card_content: 卡片内容
            
        Returns:
            发送结果
        """
        payload = {
            "msg_type": "interactive",
            "card": card_content
        }
        return self._send_message(payload)