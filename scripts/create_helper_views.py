#!/usr/bin/env python3
"""
Create helper views for easily querying latest versions of financial statements.

Views created:
- income_statement_latest
- cashflow_statement_latest
- balance_sheet_latest

Each view returns only the latest version (highest report_type) for each (code, stat_date).
"""

import sys
sys.path.insert(0, '.')

from api import create_api
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TABLES = ['income_statement', 'cashflow_statement', 'balance_sheet']

def create_latest_view(api, table_name):
    """Create a view that returns latest version for each (code, stat_date)."""
    view_name = f"{table_name}_latest"

    logger.info(f"Creating view {view_name}...")

    # Drop view if exists
    try:
        api.query(f"DROP VIEW IF EXISTS {view_name}")
    except:
        pass

    # Create view that gets latest version (max report_type) for each (code, stat_date)
    create_sql = f"""
        CREATE VIEW {view_name} AS
        SELECT * FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY code, stat_date
                       ORDER BY report_type DESC
                   ) as version_rank
            FROM {table_name}
        ) subq
        WHERE version_rank = 1
    """

    try:
        api.query(create_sql)
        logger.info(f"✅ Created view {view_name}")

        # Test the view
        count = api.query(f"SELECT COUNT(*) as cnt FROM {view_name}")['cnt'][0]
        logger.info(f"  View has {count:,} records (latest versions only)")

        return True
    except Exception as e:
        logger.error(f"❌ Failed to create view {view_name}: {e}")
        return False

def main():
    logger.info("="*80)
    logger.info("Creating Helper Views for Latest Financial Statement Versions")
    logger.info("="*80)

    api = create_api()
    api.initialize()

    try:
        for table in TABLES:
            if not create_latest_view(api, table):
                return 1

        logger.info("\n" + "="*80)
        logger.info("✅ All views created successfully!")
        logger.info("="*80)

        logger.info("\nUsage:")
        logger.info("  # Query latest versions only:")
        logger.info("  SELECT * FROM income_statement_latest WHERE code = '000001.SZ'")
        logger.info("")
        logger.info("  # Query all versions:")
        logger.info("  SELECT * FROM income_statement WHERE code = '000001.SZ'")
        logger.info("")
        logger.info("  # Query specific version:")
        logger.info("  SELECT * FROM income_statement WHERE code = '000001.SZ' AND report_type = 1")

        return 0

    except Exception as e:
        logger.error(f"\n❌ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        api.close()

if __name__ == "__main__":
    sys.exit(main())
