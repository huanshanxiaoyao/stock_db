"""更新指数列表

从JQData获取最新的中国市场指数列表并保存到数据库
"""

import sys
import logging
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from api import load_credentials
from providers.jqdata import JQDataSource
from services.stock_list_service import StockListService
from duckdb_impl import DuckDBDatabase
from config import get_config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def update_index_list(force_update: bool = False):
    """更新指数列表

    Args:
        force_update: 是否强制更新（忽略已有数据）

    Returns:
        bool: 是否成功
    """
    logger.info("=" * 70)
    logger.info("更新中国市场指数列表")
    logger.info("=" * 70)

    try:
        # 1. 加载配置和凭证
        logger.info("\n[步骤 1/4] 加载配置和认证...")
        config = get_config()
        credentials = load_credentials()

        jq_username = credentials.get("jq_username")
        jq_password = credentials.get("jq_password")

        if not jq_username or not jq_password:
            logger.error("❌ 错误: 未配置JQData凭证")
            logger.error("请在.env文件中设置 JQ_USERNAME 和 JQ_PASSWORD")
            return False

        # 2. 初始化数据源
        logger.info("\n[步骤 2/4] 初始化JQData数据源...")
        jq_source = JQDataSource()
        if not jq_source.authenticate(jq_username, jq_password):
            logger.error("❌ JQData认证失败")
            return False
        logger.info("✓ JQData认证成功")

        # 3. 初始化数据库
        logger.info("\n[步骤 3/4] 连接数据库...")
        db = DuckDBDatabase(config.database.path)
        db.connect()
        logger.info(f"✓ 数据库连接成功: {config.database.path}")

        # 4. 更新指数列表
        logger.info("\n[步骤 4/4] 获取并保存指数列表...")
        stock_list_service = StockListService(db, jq_source)

        success = stock_list_service.update_index_list(force_update=force_update)

        if success:
            # 显示统计信息
            logger.info("\n" + "=" * 70)
            logger.info("指数列表更新成功!")
            logger.info("=" * 70)

            # 获取指数统计
            all_indices = stock_list_service.get_active_indices()
            sh_indices = stock_list_service.get_active_indices(exchange='XSHG')
            sz_indices = stock_list_service.get_active_indices(exchange='XSHE')

            logger.info(f"总指数数量: {len(all_indices)}")
            logger.info(f"上海交易所: {len(sh_indices)} 个指数")
            logger.info(f"深圳交易所: {len(sz_indices)} 个指数")

            # 显示几个常见指数
            logger.info("\n常见指数:")
            common_indices = [
                ('000300.SH', '沪深300'),
                ('000001.SH', '上证指数'),
                ('399001.SZ', '深证成指'),
                ('000905.SH', '中证500')
            ]

            for code, name in common_indices:
                result = db.query_data(
                    f"SELECT code, display_name FROM stock_list WHERE code = '{code}'"
                )
                if not result.empty:
                    logger.info(f"  ✓ {name} ({code})")
                else:
                    logger.info(f"  ✗ {name} ({code}) - 未找到")

            logger.info("\n" + "=" * 70)
            logger.info("✅ 完成! 指数列表已保存到数据库")
            logger.info("=" * 70)
            logger.info("\n下一步: 运行 'python scripts\\backfill_index_prices.py' 回填历史价格数据")

            db.close()
            return True
        else:
            logger.error("\n❌ 更新指数列表失败")
            db.close()
            return False

    except Exception as e:
        logger.error(f"\n❌ 更新失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='更新中国市场指数列表')
    parser.add_argument(
        '--force',
        action='store_true',
        help='强制更新（忽略已有数据）'
    )

    args = parser.parse_args()

    try:
        success = update_index_list(force_update=args.force)
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("\n\n用户中断操作")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ 更新失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
