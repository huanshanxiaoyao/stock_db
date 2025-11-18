#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化数据平台 API 服务器启动脚本

提供便捷的API服务器启动方式，支持开发和生产环境配置。
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from api_server.server import StockDataAPIServer

def setup_logging(debug: bool = False):
    """设置日志配置"""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('api_server.log')
        ]
    )

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='量化数据平台 REST API 服务器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python start_api.py                    # 使用默认配置启动
  python start_api.py --port 8080        # 指定端口
  python start_api.py --debug             # 启用调试模式
  python start_api.py --config custom.yaml # 使用自定义配置
        """
    )
    
    parser.add_argument(
        '--host', 
        default='0.0.0.0', 
        help='API服务器监听地址 (默认: 0.0.0.0)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=5005,
        help='API服务器监听端口 (默认: 5005)'
    )
    parser.add_argument(
        '--debug', 
        action='store_true', 
        help='启用调试模式'
    )
    parser.add_argument(
        '--config', 
        default='config.yaml', 
        help='配置文件路径 (默认: config.yaml)'
    )
    parser.add_argument(
        '--workers', 
        type=int, 
        default=1, 
        help='工作进程数量 (仅用于生产环境)'
    )
    parser.add_argument(
        '--production', 
        action='store_true', 
        help='使用生产环境配置 (使用Gunicorn)'
    )
    parser.add_argument(
        '--no-replica', 
        action='store_true', 
        help='禁用数据库副本模式（使用直连模式）'
    )
    
    args = parser.parse_args()
    
    # 设置日志
    setup_logging(args.debug)
    logger = logging.getLogger(__name__)
    
    # 检查配置文件
    if not os.path.exists(args.config):
        logger.error(f"配置文件不存在: {args.config}")
        sys.exit(1)
    
    try:
        if args.production:
            # 生产环境：使用Gunicorn
            logger.info("启动生产环境API服务器...")
            import subprocess
            
            cmd = [
                'gunicorn',
                '--bind', f'{args.host}:{args.port}',
                '--workers', str(args.workers),
                '--worker-class', 'gevent',
                '--worker-connections', '1000',
                '--timeout', '30',
                '--keepalive', '2',
                '--max-requests', '1000',
                '--max-requests-jitter', '100',
                '--access-logfile', 'access.log',
                '--error-logfile', 'error.log',
                '--log-level', 'info',
                'api_server.server:create_app()'
            ]
            
            # 设置环境变量
            env = os.environ.copy()
            env['CONFIG_PATH'] = args.config
            env['USE_REPLICA'] = 'false' if args.no_replica else 'true'
            
            subprocess.run(cmd, env=env)
            
        else:
            # 开发环境：使用Flask内置服务器
            logger.info("启动开发环境API服务器...")
            use_replica = not args.no_replica
            server = StockDataAPIServer(args.config, use_replica=use_replica)
            
            logger.info(f"API服务器地址: http://{args.host}:{args.port}")
            logger.info(f"API文档地址: http://{args.host}:{args.port}/api/v1/info")
            logger.info(f"健康检查地址: http://{args.host}:{args.port}/health")
            
            server.run(host=args.host, port=args.port, debug=args.debug)
            
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在关闭服务器...")
    except Exception as e:
        logger.error(f"启动服务器失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()