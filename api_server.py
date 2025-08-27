#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化数据平台 REST API 服务器

提供标准化的RESTful API接口，支持股票数据查询、分析和管理功能。
基于现有的StockDataAPI架构，提供Web服务接口。

作者: Stock Data Platform Team
版本: 1.0.0
"""

import os
import json
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from functools import wraps

from flask import Flask, request, jsonify, g
from flask_cors import CORS
from werkzeug.exceptions import BadRequest, NotFound, InternalServerError

from api import StockDataAPI
from config import get_config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockDataAPIServer:
    """股票数据API服务器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """初始化API服务器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = get_config(config_path)
        self.app = Flask(__name__)
        
        # 启用CORS支持
        CORS(self.app)
        
        # 配置Flask
        self.app.config['JSON_AS_ASCII'] = False
        self.app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
        
        # 初始化数据API
        self.data_api = None
        
        # 设置路由
        self._setup_routes()
        self._setup_error_handlers()
        
        logger.info("股票数据API服务器初始化完成")
    
    def _get_data_api(self) -> StockDataAPI:
        """获取数据API实例（延迟初始化）"""
        if self.data_api is None:
            db_path = self.config.database.path if hasattr(self.config, 'database') else 'stock_data.duckdb'
            self.data_api = StockDataAPI(db_path)
            self.data_api.initialize()
        return self.data_api
    
    def _setup_routes(self):
        """设置API路由"""
        
        # 健康检查
        @self.app.route('/health', methods=['GET'])
        def health_check():
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'version': '1.0.0'
            })
        
        # API信息
        @self.app.route('/api/v1/info', methods=['GET'])
        def api_info():
            return jsonify({
                'name': '量化数据平台API',
                'version': '1.0.0',
                'description': '提供股票数据查询、分析和管理功能',
                'endpoints': {
                    'stocks': '/api/v1/stocks',
                    'price': '/api/v1/stocks/{code}/price',
                    'financial': '/api/v1/stocks/{code}/financial',
                    'analysis': '/api/v1/analysis',
                    'database': '/api/v1/database'
                }
            })
        
        # 股票列表
        @self.app.route('/api/v1/stocks', methods=['GET'])
        def get_stocks():
            """获取股票列表"""
            try:
                api = self._get_data_api()
                
                # 获取查询参数
                market = request.args.get('market')
                exchange = request.args.get('exchange')
                active_only = request.args.get('active_only', 'true').lower() == 'true'
                limit = request.args.get('limit', type=int)
                offset = request.args.get('offset', 0, type=int)
                
                # 获取股票代码列表
                stock_codes = api.get_stock_list(
                    market=market,
                    exchange=exchange,
                    active_only=active_only
                )
                
                # 转换为详细信息格式
                stocks = []
                for code in stock_codes:
                    stock_info = api.get_stock_info(code)
                    if stock_info:
                        stocks.append(stock_info)
                    else:
                        # 如果没有详细信息，至少返回代码
                        stocks.append({'code': code, 'name': code, 'display_name': code})
                
                # 分页处理
                total = len(stocks)
                if limit:
                    stocks = stocks[offset:offset + limit]
                
                return jsonify({
                    'success': True,
                    'data': stocks,
                    'pagination': {
                        'total': total,
                        'limit': limit,
                        'offset': offset,
                        'count': len(stocks)
                    }
                })
                
            except Exception as e:
                logger.error(f"获取股票列表失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 股票基本信息
        @self.app.route('/api/v1/stocks/<code>', methods=['GET'])
        def get_stock_info(code: str):
            """获取股票基本信息"""
            try:
                api = self._get_data_api()
                
                # 获取股票基本信息
                info = api.get_stock_info(code)
                
                if not info:
                    return jsonify({
                        'success': False,
                        'error': f'股票 {code} 不存在'
                    }), 404
                
                return jsonify({
                    'success': True,
                    'data': info
                })
                
            except Exception as e:
                logger.error(f"获取股票信息失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 价格数据
        @self.app.route('/api/v1/stocks/<code>/price', methods=['GET'])
        def get_price_data(code: str):
            """获取股票价格数据"""
            try:
                api = self._get_data_api()
                
                # 获取查询参数
                start_date = request.args.get('start_date')
                end_date = request.args.get('end_date')
                fields = request.args.get('fields', '').split(',') if request.args.get('fields') else None
                
                # 参数验证
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                
                # 获取价格数据
                data = api.get_price_data(
                    code=code,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if data.empty:
                    return jsonify({
                        'success': False,
                        'error': f'未找到股票 {code} 的价格数据'
                    }), 404
                
                # 转换为JSON格式
                result = data.to_dict('records')
                
                return jsonify({
                    'success': True,
                    'data': result,
                    'count': len(result)
                })
                
            except ValueError as e:
                return jsonify({
                    'success': False,
                    'error': f'参数错误: {e}'
                }), 400
            except Exception as e:
                logger.error(f"获取价格数据失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 财务数据
        @self.app.route('/api/v1/stocks/<code>/financial', methods=['GET'])
        def get_financial_data(code: str):
            """获取股票财务数据"""
            try:
                api = self._get_data_api()
                
                # 获取查询参数
                data_type = request.args.get('type', 'summary')  # summary, ratios
                periods = request.args.get('periods', 4, type=int)
                
                # 获取财务数据
                if data_type == 'summary':
                    # 获取所有财务数据
                    data = api.get_financial_data(code)
                    
                    if isinstance(data, dict):
                        # 财务摘要数据
                        result = {}
                        for key, df in data.items():
                            if not df.empty:
                                result[key] = df.to_dict('records')
                    else:
                        result = {}
                elif data_type == 'ratios':
                    # 获取财务比率
                    result = api.calculate_financial_ratios(code)
                else:
                    # 获取所有财务数据
                    data = api.get_financial_data(code)
                    if isinstance(data, dict) and data:
                        result = {}
                        for key, df in data.items():
                            if not df.empty:
                                result[key] = df.to_dict('records')
                    else:
                        result = {}
                
                if not result:
                    return jsonify({
                        'success': False,
                        'error': f'未找到股票 {code} 的财务数据'
                    }), 404
                
                return jsonify({
                    'success': True,
                    'data': result
                })
                
            except Exception as e:
                logger.error(f"获取财务数据失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 批量价格数据
        @self.app.route('/api/v1/stocks/batch/price', methods=['POST'])
        def get_batch_price_data():
            """批量获取股票价格数据"""
            try:
                api = self._get_data_api()
                
                # 获取请求数据
                data = request.get_json()
                if not data:
                    return jsonify({
                        'success': False,
                        'error': '请求数据不能为空'
                    }), 400
                
                codes = data.get('codes', [])
                start_date = data.get('start_date')
                end_date = data.get('end_date')
                fields = data.get('fields')
                
                if not codes:
                    return jsonify({
                        'success': False,
                        'error': '股票代码列表不能为空'
                    }), 400
                
                # 参数验证
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                
                # 批量获取数据
                results = {}
                for code in codes:
                    try:
                        price_data = api.get_price_data(
                            code=code,
                            start_date=start_date,
                            end_date=end_date
                        )
                        results[code] = price_data.to_dict('records') if not price_data.empty else []
                    except Exception as e:
                        results[code] = {'error': str(e)}
                
                return jsonify({
                    'success': True,
                    'data': results
                })
                
            except Exception as e:
                logger.error(f"批量获取价格数据失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 批量价格数据（新接口）
        @self.app.route('/api/v1/stocks/batch/prices', methods=['POST'])
        def get_batch_prices():
            """批量获取股票价格数据（新接口）"""
            try:
                api = self._get_data_api()
                
                # 获取请求参数
                data = request.get_json()
                if not data:
                    return jsonify({
                        'success': False,
                        'error': '请求参数不能为空'
                    }), 400
                
                codes = data.get('codes', [])
                start_date = data.get('start_date')
                end_date = data.get('end_date')
                fields = data.get('fields', ['open', 'high', 'low', 'close', 'volume'])
                
                if not codes:
                    return jsonify({
                        'success': False,
                        'error': '股票代码列表不能为空'
                    }), 400
                
                # 构建SQL查询
                field_str = ', '.join(fields)
                codes_str = "', '".join(codes)
                
                sql = f"SELECT code, date, {field_str} FROM price_data WHERE code IN ('{codes_str}')"
                
                if start_date:
                    sql += f" AND date >= '{start_date}'"
                if end_date:
                    sql += f" AND date <= '{end_date}'"
                
                sql += " ORDER BY code, date"
                
                # 执行查询
                df = api.query(sql)
                
                return jsonify({
                    'success': True,
                    'data': df.to_dict('records') if not df.empty else [],
                    'count': len(df)
                })
                
            except Exception as e:
                logger.error(f"批量获取价格数据失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 股票筛选
        @self.app.route('/api/v1/analysis/screen', methods=['POST'])
        def screen_stocks():
            """股票筛选"""
            try:
                api = self._get_data_api()
                
                # 获取筛选条件
                criteria = request.get_json()
                if not criteria:
                    return jsonify({
                        'success': False,
                        'error': '筛选条件不能为空'
                    }), 400
                
                # 转换筛选条件格式
                converted_criteria = {}
                for key, value in criteria.items():
                    if isinstance(value, dict) and 'min' in value:
                        converted_criteria[f'{key}_min'] = value['min']
                    if isinstance(value, dict) and 'max' in value:
                        converted_criteria[f'{key}_max'] = value['max']
                    if not isinstance(value, dict):
                        converted_criteria[key] = value
                
                # 执行筛选
                stock_codes = api.screen_stocks(converted_criteria)
                
                # 获取股票详细信息
                results = []
                for code in stock_codes:
                    stock_info = api.get_stock_info(code)
                    if stock_info:
                        results.append(stock_info)
                    else:
                        results.append({'code': code, 'name': code})
                
                return jsonify({
                    'success': True,
                    'data': results,
                    'count': len(results)
                })
                
            except Exception as e:
                logger.error(f"股票筛选失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 排行榜
        @self.app.route('/api/v1/analysis/ranking', methods=['GET'])
        def get_ranking():
            """获取股票排行榜"""
            try:
                api = self._get_data_api()
                
                # 获取查询参数
                metric = request.args.get('metric', 'market_cap')  # 排序指标
                order = request.args.get('order', 'desc')  # asc, desc
                limit = request.args.get('limit', 50, type=int)
                market = request.args.get('market')
                
                # 构建SQL查询获取排行榜
                order_clause = 'DESC' if order.lower() == 'desc' else 'ASC'
                
                # 根据指标构建查询
                if metric == 'market_cap':
                    sql = f"SELECT DISTINCT code, market_cap FROM fundamental_data WHERE market_cap IS NOT NULL ORDER BY market_cap {order_clause} LIMIT ?"
                elif metric == 'pe_ratio':
                    sql = f"SELECT DISTINCT code, pe_ratio FROM fundamental_data WHERE pe_ratio IS NOT NULL ORDER BY pe_ratio {order_clause} LIMIT ?"
                elif metric == 'pb_ratio':
                    sql = f"SELECT DISTINCT code, pb_ratio FROM fundamental_data WHERE pb_ratio IS NOT NULL ORDER BY pb_ratio {order_clause} LIMIT ?"
                else:
                    # 默认按市值排序
                    sql = f"SELECT DISTINCT code, market_cap FROM fundamental_data WHERE market_cap IS NOT NULL ORDER BY market_cap {order_clause} LIMIT ?"
                
                # 执行查询
                df = api.query(sql, [limit])
                
                if df.empty:
                    return jsonify({
                        'success': True,
                        'data': [],
                        'count': 0,
                        'metric': metric,
                        'order': order
                    })
                
                # 获取股票详细信息
                results = []
                for _, row in df.iterrows():
                    code = row['code']
                    stock_info = api.get_stock_info(code)
                    if stock_info:
                        stock_info[metric] = row.get(metric)
                        results.append(stock_info)
                    else:
                        results.append({
                            'code': code,
                            'name': code,
                            metric: row.get(metric)
                        })
                
                return jsonify({
                    'success': True,
                    'data': results,
                    'count': len(results),
                    'metric': metric,
                    'order': order
                })
                
            except Exception as e:
                logger.error(f"获取排行榜失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 数据库信息
        @self.app.route('/api/v1/database/info', methods=['GET'])
        def get_database_info():
            """获取数据库信息"""
            try:
                api = self._get_data_api()
                info = api.get_database_info()
                
                return jsonify({
                    'success': True,
                    'data': info
                })
                
            except Exception as e:
                logger.error(f"获取数据库信息失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 自定义SQL查询
        @self.app.route('/api/v1/database/query', methods=['POST'])
        def execute_query():
            """执行自定义SQL查询"""
            try:
                api = self._get_data_api()
                
                # 获取查询语句
                data = request.get_json()
                if not data or 'sql' not in data:
                    return jsonify({
                        'success': False,
                        'error': 'SQL查询语句不能为空'
                    }), 400
                
                sql = data['sql']
                params = data.get('params', [])
                
                # 安全检查：只允许SELECT查询
                if not sql.strip().upper().startswith('SELECT'):
                    return jsonify({
                        'success': False,
                        'error': '只允许执行SELECT查询'
                    }), 400
                
                # 执行查询
                result = api.query(sql, params)
                
                return jsonify({
                    'success': True,
                    'data': result.to_dict('records') if not result.empty else [],
                    'count': len(result)
                })
                
            except Exception as e:
                logger.error(f"执行查询失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
    
    def _setup_error_handlers(self):
        """设置错误处理器"""
        
        @self.app.errorhandler(400)
        def bad_request(error):
            return jsonify({
                'success': False,
                'error': '请求参数错误',
                'details': str(error)
            }), 400
        
        @self.app.errorhandler(404)
        def not_found(error):
            return jsonify({
                'success': False,
                'error': '资源未找到',
                'details': str(error)
            }), 404
        
        @self.app.errorhandler(500)
        def internal_error(error):
            return jsonify({
                'success': False,
                'error': '服务器内部错误',
                'details': str(error)
            }), 500
    
    def run(self, host: str = '0.0.0.0', port: int = 5000, debug: bool = False):
        """启动API服务器
        
        Args:
            host: 监听地址
            port: 监听端口
            debug: 是否启用调试模式
        """
        logger.info(f"启动股票数据API服务器: http://{host}:{port}")
        self.app.run(host=host, port=port, debug=debug)
    
    def get_app(self):
        """获取Flask应用实例（用于WSGI部署）"""
        return self.app


def create_app(config_path: str = "config.yaml") -> Flask:
    """创建Flask应用实例
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        Flask应用实例
    """
    server = StockDataAPIServer(config_path)
    return server.get_app()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='股票数据API服务器')
    parser.add_argument('--host', default='0.0.0.0', help='监听地址')
    parser.add_argument('--port', type=int, default=5000, help='监听端口')
    parser.add_argument('--debug', action='store_true', help='启用调试模式')
    parser.add_argument('--config', default='config.yaml', help='配置文件路径')
    
    args = parser.parse_args()
    
    # 创建并启动服务器
    server = StockDataAPIServer(args.config)
    server.run(host=args.host, port=args.port, debug=args.debug)