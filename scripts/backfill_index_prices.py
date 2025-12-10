"""回填指数价格数据 (从2019-01-01开始)

与股票价格数据保持相同的起始日期
"""

import sys
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from providers.jqdata import JQDataSource
from services.stock_list_service import StockListService
from duckdb_impl import DuckDBDatabase
from config import get_config
from api import load_credentials

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def backfill_index_prices(start_date: date, end_date: date = None,
                          batch_size: int = 20, exchange: str = None):
    """回填指数价格数据

    Args:
        start_date: 开始日期
        end_date: 结束日期 (默认为今天)
        batch_size: 每批处理的指数数量
        exchange: 交易所过滤 (None=全部, 'XSHG'=上海, 'XSHE'=深圳)
    """
    if end_date is None:
        end_date = date.today()

    logger.info("=" * 70)
    logger.info("指数价格数据回填任务")
    logger.info("=" * 70)
    logger.info(f"时间范围: {start_date} 至 {end_date}")
    logger.info(f"批次大小: {batch_size}")
    if exchange:
        logger.info(f"交易所: {exchange}")
    logger.info("=" * 70)

    # 1. 加载配置和凭证
    logger.info("\n[步骤 1/5] 加载配置和认证...")
    config = get_config()
    credentials = load_credentials()

    jq_username = credentials.get("jq_username")
    jq_password = credentials.get("jq_password")

    if not jq_username or not jq_password:
        logger.error("❌ 错误: 未配置JQData凭证")
        return False

    # 2. 初始化数据源和数据库
    logger.info("\n[步骤 2/5] 初始化数据源和数据库...")
    jq_source = JQDataSource()
    if not jq_source.authenticate(jq_username, jq_password):
        logger.error("❌ JQData认证失败")
        return False
    logger.info("✓ JQData认证成功")

    db = DuckDBDatabase(config.database.path)
    db.connect()
    logger.info(f"✓ 数据库连接成功: {config.database.path}")

    try:
        # 3. 获取指数列表
        logger.info("\n[步骤 3/5] 获取指数列表...")
        stock_list_service = StockListService(db, jq_source)

        # 获取活跃指数代码
        index_codes = stock_list_service.get_index_codes(exchange=exchange)

        if not index_codes:
            logger.warning("⚠️  未找到指数，请先运行 update_index_list()")
            return False

        logger.info(f"✓ 找到 {len(index_codes)} 个活跃指数")

        # 4. 检查哪些指数已有数据，哪些需要回填
        logger.info("\n[步骤 4/5] 检查现有数据...")
        indices_to_fetch = []
        indices_with_data = []

        # Calculate expected trading days (approximately)
        days_diff = (end_date - start_date).days
        # Assume ~245 trading days per year
        expected_min_records = int(days_diff * 245 / 365 * 0.8)  # 80% threshold

        for code in index_codes:
            # 查询该指数的数据情况
            result = db.query_data(f"""
                SELECT
                    COUNT(*) as cnt,
                    MIN(day) as earliest,
                    MAX(day) as latest
                FROM price_data
                WHERE code = '{code}'
            """)

            if result.empty or result['cnt'][0] == 0:
                # No data at all
                indices_to_fetch.append(code)
            else:
                cnt = result['cnt'][0]
                earliest = result['earliest'][0]

                # Check if data is complete:
                # 1. Has sufficient records
                # 2. Earliest date is close to start_date (within 60 days)
                if cnt < expected_min_records or (earliest and earliest > start_date + timedelta(days=60)):
                    indices_to_fetch.append(code)
                else:
                    indices_with_data.append(code)

        logger.info(f"✓ {len(indices_with_data)} 个指数已有数据")
        logger.info(f"✓ {len(indices_to_fetch)} 个指数需要回填")

        if not indices_to_fetch:
            logger.info("\n✅ 所有指数都已有价格数据，无需回填")
            return True

        # 5. 批量获取并保存价格数据
        logger.info(f"\n[步骤 5/5] 批量回填价格数据 (每批 {batch_size} 个指数)...")

        total_batches = (len(indices_to_fetch) + batch_size - 1) // batch_size
        total_records = 0
        success_count = 0
        failed_indices = []

        for i in range(0, len(indices_to_fetch), batch_size):
            batch_codes = indices_to_fetch[i:i + batch_size]
            batch_num = i // batch_size + 1

            logger.info(f"\n批次 {batch_num}/{total_batches}: 处理 {len(batch_codes)} 个指数...")
            logger.info(f"代码: {', '.join(batch_codes[:5])}{'...' if len(batch_codes) > 5 else ''}")

            try:
                # 获取价格数据
                df_price = jq_source.get_index_price_data(
                    codes=batch_codes,
                    start_date=start_date,
                    end_date=end_date
                )

                if df_price.empty:
                    logger.warning(f"  ⚠️  批次 {batch_num}: 未获取到数据")
                    failed_indices.extend(batch_codes)
                    continue

                # 保存到数据库
                success = db.insert_dataframe(df_price, 'price_data')

                if success:
                    records = len(df_price)
                    total_records += records
                    success_count += len(batch_codes)
                    logger.info(f"  ✓ 批次 {batch_num}: 保存了 {records} 条记录")
                else:
                    logger.error(f"  ❌ 批次 {batch_num}: 保存失败")
                    failed_indices.extend(batch_codes)

            except Exception as e:
                logger.error(f"  ❌ 批次 {batch_num} 失败: {e}")
                failed_indices.extend(batch_codes)

        # 6. 总结
        logger.info("\n" + "=" * 70)
        logger.info("回填任务完成")
        logger.info("=" * 70)
        logger.info(f"总指数数量: {len(index_codes)}")
        logger.info(f"已有数据: {len(indices_with_data)}")
        logger.info(f"需要回填: {len(indices_to_fetch)}")
        logger.info(f"成功回填: {success_count}")
        logger.info(f"失败: {len(failed_indices)}")
        logger.info(f"总记录数: {total_records}")
        logger.info("=" * 70)

        if failed_indices:
            logger.warning(f"\n失败的指数 ({len(failed_indices)}):")
            for code in failed_indices[:20]:
                logger.warning(f"  - {code}")
            if len(failed_indices) > 20:
                logger.warning(f"  ... 还有 {len(failed_indices) - 20} 个")

        return len(failed_indices) == 0

    finally:
        db.close()
        logger.info("\n数据库连接已关闭")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='回填指数价格数据')
    parser.add_argument(
        '--start-date',
        type=str,
        default='2019-01-01',
        help='开始日期 (YYYY-MM-DD), 默认: 2019-01-01'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='结束日期 (YYYY-MM-DD), 默认: 今天'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=20,
        help='每批处理的指数数量, 默认: 20'
    )
    parser.add_argument(
        '--exchange',
        type=str,
        choices=['XSHG', 'XSHE'],
        help='交易所过滤 (XSHG=上海, XSHE=深圳)'
    )

    args = parser.parse_args()

    # 解析日期
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date() if args.end_date else None

    # 执行回填
    try:
        success = backfill_index_prices(
            start_date=start_date,
            end_date=end_date,
            batch_size=args.batch_size,
            exchange=args.exchange
        )

        sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"\n❌ 回填任务失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
