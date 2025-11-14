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
import pandas as pd
import numpy as np
from flask_cors import CORS
from werkzeug.exceptions import BadRequest, NotFound, InternalServerError

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api import StockDataAPI
from config import get_config

# 获取日志记录器（不重复配置basicConfig，避免覆盖start.py中的配置）
logger = logging.getLogger(__name__)

def safe_json_convert(df):
    """安全的DataFrame转JSON转换，处理pandas数据类型"""
    if df.empty:
        return []

    # 将DataFrame转换为records格式，同时处理数据类型
    records = df.to_dict('records')

    # 转换numpy/pandas数据类型为Python原生类型
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            elif isinstance(value, (np.integer, np.int64)):
                record[key] = int(value)
            elif isinstance(value, (np.floating, np.float64)):
                record[key] = float(value)
            elif isinstance(value, np.bool_):
                record[key] = bool(value)

    return records

class StockDataAPIServer:
    """股票数据API服务器"""

    def __init__(self, config_path: str = "config.yaml", use_replica: bool = True):
        """初始化API服务器
        
        Args:
            config_path: 配置文件路径
            use_replica: 是否使用数据库副本模式
        """
        self.config = get_config(config_path)
        self.use_replica = use_replica
        self.app = Flask(__name__)
        
        # 启用CORS支持
        CORS(self.app)
        
        # 配置Flask
        self.app.config['JSON_AS_ASCII'] = False
        self.app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
        
        # 初始化数据API（立即初始化而不是延迟）
        db_path = self.config.database.path if hasattr(self.config, 'database') else 'stock_data.duckdb'
        
        # 构建配置字典，包含数据源配置
        api_config = {}
        if hasattr(self.config, 'data_sources'):
            # 将数据源配置转换为字典格式
            data_sources_config = []
            for source_name, source_config in self.config.data_sources.items():
                if hasattr(source_config, 'enabled') and source_config.enabled:
                    source_dict = {
                        'type': source_name,
                        'name': source_name,  # 添加name字段
                        'enabled': True,
                        'default': True  # 设置为默认数据源
                    }
                    # 添加其他配置项
                    if hasattr(source_config, 'api_config'):
                        source_dict['api_config'] = source_config.api_config
                    data_sources_config.append(source_dict)
            
            if data_sources_config:
                api_config['data_sources'] = data_sources_config
        
        self.data_api = StockDataAPI(db_path, api_config, use_replica=self.use_replica)
        self.data_api.initialize()

        # 设置路由
        self._setup_routes()
        self._setup_error_handlers()

        if use_replica:
            logger.info("股票数据API服务器初始化完成（副本模式），监控线程已启动")
        else:
            logger.info("股票数据API服务器初始化完成（直连模式）")
    
    def _get_data_api(self) -> StockDataAPI:
        """获取数据API实例"""
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
                    'batch_prices': '/api/v1/stocks/batch/prices',
                    'transactions': '/api/v1/transactions',
                    'recent_transactions': '/api/v1/transactions/recent',
                    'positions': '/api/v1/positions',
                    'accounts': '/api/v1/accounts',
                    'database_info': '/api/v1/database/info'
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
                        logger.info(f"未找到股票 {code} 的详细信息，使用默认值")
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
                result = safe_json_convert(data)
                
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
                data_type = request.args.get('type', 'summary')  # valuation, indicator, summary
                periods = request.args.get('periods', 4, type=int)

                # 获取日期范围参数
                start_date = request.args.get('start_date')
                end_date = request.args.get('end_date')

                # 解析日期参数
                start_date_obj = None
                end_date_obj = None

                if start_date:
                    try:
                        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
                    except ValueError:
                        return jsonify({
                            'success': False,
                            'error': 'start_date格式错误，请使用YYYY-MM-DD格式'
                        }), 400

                if end_date:
                    try:
                        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                    except ValueError:
                        return jsonify({
                            'success': False,
                            'error': 'end_date格式错误，请使用YYYY-MM-DD格式'
                        }), 400

                # 根据类型获取特定的财务数据
                result = {}

                if data_type == 'valuation':
                    # 只获取估值数据
                    try:
                        sql = "SELECT * FROM valuation_data WHERE code = ?"
                        params = [code]

                        if start_date_obj:
                            sql += " AND day >= ?"
                            params.append(start_date_obj)

                        if end_date_obj:
                            sql += " AND day <= ?"
                            params.append(end_date_obj)

                        sql += " ORDER BY day DESC LIMIT 10"

                        df = api.query(sql, params)
                        if not df.empty:
                            result['valuation_data'] = safe_json_convert(df)
                    except Exception as e:
                        logger.debug(f"获取valuation_data失败: {e}")

                elif data_type == 'indicator':
                    # 只获取指标数据
                    # indicator_data是季度/定期数据，如果指定日期范围内没有数据，则返回最近的数据
                    try:
                        sql = "SELECT * FROM indicator_data WHERE code = ?"
                        params = [code]

                        # 首先尝试在指定日期范围内查询
                        date_filtered = False
                        if start_date_obj or end_date_obj:
                            date_sql = sql
                            date_params = params.copy()

                            if start_date_obj:
                                date_sql += " AND pubDate >= ?"
                                date_params.append(start_date_obj)

                            if end_date_obj:
                                date_sql += " AND pubDate <= ?"
                                date_params.append(end_date_obj)

                            date_sql += " ORDER BY pubDate DESC LIMIT 10"
                            df = api.query(date_sql, date_params)

                            if not df.empty:
                                result['indicator_data'] = safe_json_convert(df)
                                date_filtered = True

                        # 如果日期范围内没有数据，返回最近的10条数据
                        if not date_filtered:
                            sql += " ORDER BY pubDate DESC LIMIT 10"
                            df = api.query(sql, params)
                            if not df.empty:
                                result['indicator_data'] = safe_json_convert(df)
                    except Exception as e:
                        logger.debug(f"获取indicator_data失败: {e}")

                elif data_type == 'summary':
                    # 获取所有财务数据，传递日期参数
                    data = api.get_financial_data(code, start_date_obj, end_date_obj)

                    if isinstance(data, dict):
                        # 财务摘要数据
                        for key, df in data.items():
                            if not df.empty:
                                result[key] = safe_json_convert(df)

                elif data_type == 'ratios':
                    # 财务比率功能已移除
                    result = {}
                else:
                    return jsonify({
                        'success': False,
                        'error': f'不支持的数据类型: {data_type}，支持的类型: valuation, indicator, summary'
                    }), 400

                if not result:
                    return jsonify({
                        'success': False,
                        'error': f'未找到股票 {code} 的 {data_type} 数据'
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
                        results[code] = safe_json_convert(price_data) if not price_data.empty else []
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

                sql = f"SELECT code, day as date, {field_str} FROM price_data WHERE code IN ('{codes_str}')"

                if start_date:
                    sql += f" AND day >= '{start_date}'"
                if end_date:
                    sql += f" AND day <= '{end_date}'"

                sql += " ORDER BY code, day"
                
                # 执行查询
                df = api.query(sql)
                
                return jsonify({
                    'success': True,
                    'data': safe_json_convert(df) if not df.empty else [],
                    'count': int(len(df))
                })
                
            except Exception as e:
                logger.error(f"批量获取价格数据失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        
        
        # ==================== 用户交易记录相关API ====================
        
        # 获取用户交易记录
        @self.app.route('/api/v1/transactions', methods=['GET'])
        def get_user_transactions():
            """获取用户交易记录"""
            try:
                api = self._get_data_api()
                
                # 获取查询参数
                user_id = request.args.get('user_id')
                stock_code = request.args.get('stock_code')
                trade_date = request.args.get('trade_date')
                start_date = request.args.get('start_date')
                end_date = request.args.get('end_date')
                trade_type = request.args.get('trade_type', type=int)
                limit = request.args.get('limit', 100, type=int)
                offset = request.args.get('offset', 0, type=int)
                
                # 参数验证
                if trade_date:
                    trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                
                # 构建查询条件
                conditions = []
                params = []
                
                if user_id:
                    conditions.append("user_id = ?")
                    params.append(user_id)
                if stock_code:
                    conditions.append("stock_code = ?")
                    params.append(stock_code)
                if trade_date:
                    conditions.append("trade_date = ?")
                    params.append(trade_date)
                if start_date:
                    conditions.append("trade_date >= ?")
                    params.append(start_date)
                if end_date:
                    conditions.append("trade_date <= ?")
                    params.append(end_date)
                if trade_type is not None:
                    conditions.append("trade_type = ?")
                    params.append(trade_type)
                
                # 构建SQL查询
                where_clause = " AND ".join(conditions) if conditions else "1=1"
                count_sql = f"SELECT COUNT(*) as total FROM user_transactions WHERE {where_clause}"
                
                # 获取总数
                count_result = api.query(count_sql, params)
                total = int(count_result.iloc[0]['total']) if not count_result.empty else 0
                
                # 构建分页查询
                sql = f"SELECT * FROM user_transactions WHERE {where_clause} ORDER BY trade_date DESC, trade_time DESC"
                if limit:
                    sql += f" LIMIT {limit} OFFSET {offset}"
                
                # 执行查询
                result = api.query(sql, params)
                
                return jsonify({
                    'success': True,
                    'data': safe_json_convert(result) if not result.empty else [],
                    'pagination': {
                        'total': total,
                        'limit': limit,
                        'offset': offset,
                        'count': int(len(result))
                    }
                })
                
            except ValueError as e:
                return jsonify({
                    'success': False,
                    'error': f'参数错误: {e}'
                }), 400
            except Exception as e:
                logger.error(f"获取用户交易记录失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 获取用户最近N天的交易记录
        @self.app.route('/api/v1/transactions/recent', methods=['GET'])
        def get_recent_transactions():
            """获取用户最近N天的交易记录"""
            try:
                api = self._get_data_api()
                
                # 获取查询参数
                user_id = request.args.get('user_id')
                days = request.args.get('days', 7, type=int)
                stock_code = request.args.get('stock_code')
                trade_type = request.args.get('trade_type', type=int)
                limit = request.args.get('limit', 1000, type=int)
                
                # 参数验证
                if not user_id:
                    return jsonify({
                        'success': False,
                        'error': '用户ID不能为空'
                    }), 400
                
                if days <= 0 or days > 365:
                    return jsonify({
                        'success': False,
                        'error': '天数必须在1-365之间'
                    }), 400
                
                # 计算日期范围
                from datetime import timedelta
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=days)
                
                # 构建查询条件
                conditions = ["user_id = ?", "trade_date >= ?", "trade_date <= ?"]
                params = [user_id, start_date, end_date]
                
                if stock_code:
                    conditions.append("stock_code = ?")
                    params.append(stock_code)
                if trade_type is not None:
                    conditions.append("trade_type = ?")
                    params.append(trade_type)
                
                # 构建SQL查询
                where_clause = " AND ".join(conditions)
                sql = f"SELECT * FROM user_transactions WHERE {where_clause} ORDER BY trade_date DESC, trade_time DESC LIMIT {limit}"
                
                # 执行查询
                result = api.query(sql, params)
                
                return jsonify({
                    'success': True,
                    'data': safe_json_convert(result) if not result.empty else [],
                    'query_info': {
                        'user_id': user_id,
                        'days': days,
                        'start_date': str(start_date),
                        'end_date': str(end_date),
                        'stock_code': stock_code,
                        'trade_type': trade_type,
                        'count': int(len(result))
                    }
                })
                
            except Exception as e:
                logger.error(f"获取最近交易记录失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # ==================== 用户持仓相关API ====================
        
        # 获取用户持仓记录
        @self.app.route('/api/v1/positions', methods=['GET'])
        def get_user_positions():
            """获取用户持仓记录"""
            try:
                api = self._get_data_api()
                
                # 获取查询参数
                user_id = request.args.get('user_id')
                stock_code = request.args.get('stock_code')
                position_date = request.args.get('position_date')
                start_date = request.args.get('start_date')
                end_date = request.args.get('end_date')
                limit = request.args.get('limit', type=int)
                offset = request.args.get('offset', 0, type=int)
                
                # 参数验证
                if position_date:
                    position_date = datetime.strptime(position_date, '%Y-%m-%d').date()
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                
                # 构建查询条件
                conditions = []
                params = []
                
                if user_id:
                    conditions.append("user_id = ?")
                    params.append(user_id)
                if stock_code:
                    conditions.append("stock_code = ?")
                    params.append(stock_code)
                if position_date:
                    conditions.append("position_date = ?")
                    params.append(position_date)
                if start_date:
                    conditions.append("position_date >= ?")
                    params.append(start_date)
                if end_date:
                    conditions.append("position_date <= ?")
                    params.append(end_date)
                
                # 构建SQL查询
                where_clause = " AND ".join(conditions) if conditions else "1=1"
                count_sql = f"SELECT COUNT(*) as total FROM user_positions WHERE {where_clause}"
                
                # 获取总数
                count_result = api.query(count_sql, params)
                total = int(count_result.iloc[0]['total']) if not count_result.empty else 0
                
                # 构建分页查询
                sql = f"SELECT * FROM user_positions WHERE {where_clause} ORDER BY position_date DESC, user_id, stock_code"
                if limit:
                    sql += f" LIMIT {limit} OFFSET {offset}"
                
                # 执行查询
                result = api.query(sql, params)
                
                return jsonify({
                    'success': True,
                    'data': safe_json_convert(result) if not result.empty else [],
                    'pagination': {
                        'total': total,
                        'limit': limit,
                        'offset': offset,
                        'count': int(len(result))
                    }
                })
                
            except ValueError as e:
                return jsonify({
                    'success': False,
                    'error': f'参数错误: {e}'
                }), 400
            except Exception as e:
                logger.error(f"获取用户持仓记录失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 获取用户账户信息
        @self.app.route('/api/v1/accounts', methods=['GET'])
        def get_user_accounts():
            """获取用户账户信息"""
            try:
                api = self._get_data_api()
                
                # 获取查询参数
                user_id = request.args.get('user_id')
                info_date = request.args.get('info_date')
                start_date = request.args.get('start_date')
                end_date = request.args.get('end_date')
                limit = request.args.get('limit', type=int)
                offset = request.args.get('offset', 0, type=int)
                
                # 参数验证
                if info_date:
                    info_date = datetime.strptime(info_date, '%Y-%m-%d').date()
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                
                # 构建查询条件
                conditions = []
                params = []
                
                if user_id:
                    conditions.append("user_id = ?")
                    params.append(user_id)
                if info_date:
                    conditions.append("info_date = ?")
                    params.append(info_date)
                if start_date:
                    conditions.append("info_date >= ?")
                    params.append(start_date)
                if end_date:
                    conditions.append("info_date <= ?")
                    params.append(end_date)
                
                # 构建SQL查询
                where_clause = " AND ".join(conditions) if conditions else "1=1"
                count_sql = f"SELECT COUNT(*) as total FROM user_account_info WHERE {where_clause}"
                
                # 获取总数
                count_result = api.query(count_sql, params)
                total = int(count_result.iloc[0]['total']) if not count_result.empty else 0
                
                # 构建分页查询
                sql = f"SELECT * FROM user_account_info WHERE {where_clause} ORDER BY info_date DESC, user_id"
                if limit:
                    sql += f" LIMIT {limit} OFFSET {offset}"
                
                # 执行查询
                result = api.query(sql, params)
                
                return jsonify({
                    'success': True,
                    'data': safe_json_convert(result) if not result.empty else [],
                    'pagination': {
                        'total': total,
                        'limit': limit,
                        'offset': offset,
                        'count': int(len(result))
                    }
                })
                
            except ValueError as e:
                return jsonify({
                    'success': False,
                    'error': f'参数错误: {e}'
                }), 400
            except Exception as e:
                logger.error(f"获取用户账户信息失败: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        # 获取用户持仓汇总
        @self.app.route('/api/v1/positions/summary', methods=['GET'])
        def get_positions_summary():
            """获取用户持仓汇总信息"""
            try:
                api = self._get_data_api()
                
                # 获取查询参数
                user_id = request.args.get('user_id')
                position_date = request.args.get('position_date')
                
                if not user_id:
                    return jsonify({
                        'success': False,
                        'error': '用户ID不能为空'
                    }), 400
                
                # 参数验证
                if position_date:
                    position_date = datetime.strptime(position_date, '%Y-%m-%d').date()
                else:
                    # 默认使用最新日期
                    latest_date_sql = "SELECT MAX(position_date) as latest_date FROM user_positions WHERE user_id = ?"
                    latest_result = api.query(latest_date_sql, [user_id])
                    if not latest_result.empty and latest_result.iloc[0]['latest_date']:
                        position_date = latest_result.iloc[0]['latest_date']
                    else:
                        return jsonify({
                            'success': False,
                            'error': f'用户 {user_id} 没有持仓记录'
                        }), 404
                
                # 获取持仓汇总
                summary_sql = """
                SELECT 
                    COUNT(*) as total_positions,
                    COUNT(DISTINCT stock_code) as unique_stocks,
                    SUM(current_quantity) as total_quantity,
                    SUM(market_value) as total_market_value,
                    SUM(unrealized_pnl) as total_unrealized_pnl,
                    AVG(unrealized_pnl_ratio) as avg_pnl_ratio
                FROM user_positions 
                WHERE user_id = ? AND position_date = ? AND current_quantity > 0
                """
                
                summary_result = api.query(summary_sql, [user_id, position_date])
                
                if summary_result.empty:
                    return jsonify({
                        'success': False,
                        'error': f'用户 {user_id} 在 {position_date} 没有持仓记录'
                    }), 404
                
                # 获取前5大持仓
                top_positions_sql = """
                SELECT stock_code, stock_name, current_quantity, market_value, unrealized_pnl, unrealized_pnl_ratio
                FROM user_positions 
                WHERE user_id = ? AND position_date = ? AND current_quantity > 0
                ORDER BY market_value DESC 
                LIMIT 5
                """
                
                top_positions = api.query(top_positions_sql, [user_id, position_date])
                
                return jsonify({
                    'success': True,
                    'data': {
                        'user_id': user_id,
                        'position_date': str(position_date),
                        'summary': safe_json_convert(summary_result)[0],
                        'top_positions': safe_json_convert(top_positions) if not top_positions.empty else []
                    }
                })
                
            except ValueError as e:
                return jsonify({
                    'success': False,
                    'error': f'参数错误: {e}'
                }), 400
            except Exception as e:
                logger.error(f"获取持仓汇总失败: {e}")
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


def create_app(config_path: str = "config.yaml", use_replica: bool = True) -> Flask:
    """
    创建Flask应用实例
    
    Args:
        config_path: 配置文件路径
        use_replica: 是否使用数据库副本模式
        
    Returns:
        Flask应用实例
    """
    # 从环境变量读取配置（用于生产环境）
    config_path = os.environ.get('CONFIG_PATH', config_path)
    use_replica_env = os.environ.get('USE_REPLICA', 'true').lower()
    use_replica = use_replica_env == 'true'
    
    server = StockDataAPIServer(config_path, use_replica=use_replica)
    return server.get_app()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='股票数据API服务器')
    parser.add_argument('--host', default='0.0.0.0', help='监听地址')
    parser.add_argument('--port', type=int, default=5000, help='监听端口')
    parser.add_argument('--debug', action='store_true', help='启用调试模式')
    parser.add_argument('--config', default='config.yaml', help='配置文件路径')
    parser.add_argument('--no-replica', action='store_true', help='禁用数据库副本模式（使用直连模式）')
    
    args = parser.parse_args()
    
    # 创建并启动服务器
    use_replica = not args.no_replica
    server = StockDataAPIServer(args.config, use_replica=use_replica)
    server.run(host=args.host, port=args.port, debug=args.debug)