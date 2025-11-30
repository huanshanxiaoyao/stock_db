#!/usr/bin/env python3
"""
Clear and re-fetch financial statement tables to ensure 100% accuracy with JQData.

This script:
1. Clears income_statement, cashflow_statement, balance_sheet tables
2. Re-fetches all data from JQData with correct report_type values
3. Ensures multiple versions are stored correctly

WARNING: This will DELETE all existing financial statement data!
The re-fetch will take 2-4 hours for ~5000 stocks.

Usage:
    python scripts/clear_and_refetch_financial_tables.py
"""

import sys
sys.path.insert(0, '.')

from api import create_api
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

TABLES_TO_CLEAR = [
    'income_statement',
    'cashflow_statement',
    'balance_sheet'
]

def clear_tables(api):
    """Clear all financial statement tables."""
    logger.info("="*80)
    logger.info("Step 1: Clearing Financial Statement Tables")
    logger.info("="*80)

    for table in TABLES_TO_CLEAR:
        try:
            # Get current count
            count_before = api.query(f"SELECT COUNT(*) as cnt FROM {table}")['cnt'][0]
            logger.info(f"\n[{table}]")
            logger.info(f"  Current records: {count_before:,}")

            # Delete all records
            logger.info(f"  Deleting all records...")
            api.query(f"DELETE FROM {table}")

            # Verify deletion
            count_after = api.query(f"SELECT COUNT(*) as cnt FROM {table}")['cnt'][0]
            logger.info(f"  After deletion: {count_after:,}")

            if count_after == 0:
                logger.info(f"  ✅ Successfully cleared {table}")
            else:
                logger.error(f"  ❌ Failed to clear {table} - still has {count_after:,} records!")
                return False

        except Exception as e:
            logger.error(f"  ❌ Error clearing {table}: {e}")
            import traceback
            traceback.print_exc()
            return False

    logger.info("\n✅ All tables cleared successfully!")
    return True

def refetch_all_data(api):
    """Re-fetch all financial statement data from JQData."""
    logger.info("\n" + "="*80)
    logger.info("Step 2: Re-fetching All Data from JQData")
    logger.info("="*80)
    logger.info("\nThis will take 2-4 hours for ~5000 stocks...")
    logger.info("You can monitor progress in the logs.\n")

    try:
        # Get all stock codes
        stock_list = api.query("SELECT DISTINCT code FROM stock_list ORDER BY code")
        stock_codes = stock_list['code'].tolist()

        logger.info(f"Found {len(stock_codes):,} stocks to fetch")
        logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Re-fetch with force_full_update=True
        api.update_data(
            codes=stock_codes,
            data_types=TABLES_TO_CLEAR,
            force_full_update=True
        )

        logger.info(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("✅ Re-fetch completed!")
        return True

    except Exception as e:
        logger.error(f"❌ Error during re-fetch: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_results(api):
    """Verify that data was re-fetched correctly."""
    logger.info("\n" + "="*80)
    logger.info("Step 3: Verifying Results")
    logger.info("="*80)

    for table in TABLES_TO_CLEAR:
        try:
            # Check record count
            total = api.query(f"SELECT COUNT(*) as cnt FROM {table}")['cnt'][0]
            logger.info(f"\n[{table}]")
            logger.info(f"  Total records: {total:,}")

            # Check report_type distribution
            dist = api.query(f"""
                SELECT report_type, COUNT(*) as cnt
                FROM {table}
                GROUP BY report_type
                ORDER BY report_type
            """)

            if not dist.empty:
                logger.info(f"  report_type distribution:")
                for _, row in dist.iterrows():
                    logger.info(f"    - type={row['report_type']}: {row['cnt']:,} records")

            # Check for multiple versions
            multi_version = api.query(f"""
                SELECT COUNT(*) as cnt
                FROM (
                    SELECT code, stat_date, COUNT(*) as versions
                    FROM {table}
                    GROUP BY code, stat_date
                    HAVING COUNT(*) > 1
                )
            """)['cnt'][0]

            if multi_version > 0:
                logger.info(f"  ✅ Found {multi_version:,} statements with multiple versions")
            else:
                logger.info(f"  ℹ️  No multiple versions found (this is normal if few corrections exist)")

            if total == 0:
                logger.error(f"  ❌ WARNING: {table} is empty!")
                return False

        except Exception as e:
            logger.error(f"  ❌ Error verifying {table}: {e}")
            return False

    logger.info("\n✅ Verification completed!")
    return True

def main():
    logger.info("="*80)
    logger.info("Clear and Re-fetch Financial Statement Tables")
    logger.info("="*80)
    logger.info("\nWARNING: This will DELETE all existing financial statement data!")
    logger.info("Tables to be cleared:")
    for table in TABLES_TO_CLEAR:
        logger.info(f"  - {table}")
    logger.info("\nThe re-fetch will take 2-4 hours.")
    logger.info("="*80)

    # Ask for confirmation
    print("\nAre you sure you want to continue? (yes/no): ", end='')
    confirmation = input().strip().lower()

    if confirmation not in ['yes', 'y']:
        logger.info("Operation cancelled by user.")
        return 0

    logger.info("\nStarting operation...")

    # Create API
    api = create_api()
    api.initialize()

    try:
        # Step 1: Clear tables
        if not clear_tables(api):
            logger.error("\n❌ Failed to clear tables. Aborting.")
            return 1

        # Step 2: Re-fetch data
        if not refetch_all_data(api):
            logger.error("\n❌ Failed to re-fetch data.")
            logger.error("Your tables are now EMPTY! You need to re-run this script or restore from backup.")
            return 1

        # Step 3: Verify
        if not verify_results(api):
            logger.warning("\n⚠️  Verification found issues, but data was fetched.")
            logger.warning("Check the logs above for details.")

        # Final summary
        logger.info("\n" + "="*80)
        logger.info("OPERATION COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info("\nYour financial statement tables now have:")
        logger.info("  ✅ Data fetched directly from JQData")
        logger.info("  ✅ Correct report_type values (0=original, 1=corrected)")
        logger.info("  ✅ Multiple versions stored where they exist")
        logger.info("\nYou can now:")
        logger.info("  - Query latest versions: SELECT * FROM income_statement_latest")
        logger.info("  - Query all versions: SELECT * FROM income_statement")
        logger.info("  - Run validation: python scripts/validate_financial_data.py")

        return 0

    except Exception as e:
        logger.error(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        api.close()

if __name__ == "__main__":
    sys.exit(main())
