#!/usr/bin/env python3
"""
Migration: Add report_type column and update PRIMARY KEY to support multiple versions.

This migration allows storing multiple versions (original + corrected) of financial statements.

Steps:
1. Add report_type column to tables
2. Recreate tables with new PRIMARY KEY (code, stat_date, report_type)
3. Verify data integrity
"""

import sys
sys.path.insert(0, '.')

from api import create_api
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TABLES = ['income_statement', 'cashflow_statement', 'balance_sheet']

def get_table_schema(api, table_name):
    """Get current table schema."""
    sql = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    return api.query(sql)

def add_report_type_column(api, table_name):
    """Add report_type column if not exists."""
    logger.info(f"[{table_name}] Checking if report_type column exists...")

    # Check if column exists
    schema = get_table_schema(api, table_name)
    if 'report_type' in schema['column_name'].values:
        logger.info(f"[{table_name}] report_type column already exists")

        # Check for NULL values and fix them
        null_count = api.query(f"SELECT COUNT(*) as cnt FROM {table_name} WHERE report_type IS NULL")['cnt'][0]
        if null_count > 0:
            logger.info(f"[{table_name}] Found {null_count:,} NULL report_type values, setting to 0...")
            api.query(f"UPDATE {table_name} SET report_type = 0 WHERE report_type IS NULL")
            logger.info(f"[{table_name}] ✅ Fixed NULL values")

        return True

    # Add column
    logger.info(f"[{table_name}] Adding report_type column...")
    try:
        sql = f"ALTER TABLE {table_name} ADD COLUMN report_type INTEGER DEFAULT 0"
        api.query(sql)

        # Set default value for all existing rows
        api.query(f"UPDATE {table_name} SET report_type = 0 WHERE report_type IS NULL")

        logger.info(f"[{table_name}] ✅ Added report_type column")
        return True
    except Exception as e:
        logger.error(f"[{table_name}] ❌ Failed to add column: {e}")
        return False

def recreate_table_with_new_pk(api, table_name):
    """Recreate table with updated PRIMARY KEY."""
    logger.info(f"[{table_name}] Recreating table with new PRIMARY KEY...")

    # Get current schema
    schema = get_table_schema(api, table_name)

    # Get row count before
    count_before = api.query(f"SELECT COUNT(*) as cnt FROM {table_name}")['cnt'][0]
    logger.info(f"[{table_name}] Current row count: {count_before:,}")

    # Build column definitions
    col_defs = []
    for _, row in schema.iterrows():
        col_name = row['column_name']
        data_type = row['data_type']
        nullable = row['is_nullable']

        # Special handling for PRIMARY KEY columns
        if col_name in ['code', 'stat_date', 'report_type']:
            col_def = f"{col_name} {data_type} NOT NULL"
        elif nullable == 'YES':
            col_def = f"{col_name} {data_type}"
        else:
            col_def = f"{col_name} {data_type} NOT NULL"

        col_defs.append(col_def)

    # Create new table with updated PRIMARY KEY
    new_table = f"{table_name}_new"
    create_sql = f"""
        CREATE TABLE {new_table} (
            {', '.join(col_defs)},
            PRIMARY KEY (code, stat_date, report_type)
        )
    """

    try:
        # Drop new table if exists (cleanup from previous failed attempt)
        try:
            api.query(f"DROP TABLE IF EXISTS {new_table}")
        except:
            pass

        # Create new table
        api.query(create_sql)
        logger.info(f"[{table_name}] Created temporary table {new_table}")

        # Copy data
        copy_sql = f"INSERT INTO {new_table} SELECT * FROM {table_name}"
        api.query(copy_sql)

        # Verify row count
        count_after = api.query(f"SELECT COUNT(*) as cnt FROM {new_table}")['cnt'][0]
        logger.info(f"[{table_name}] Copied {count_after:,} rows to new table")

        if count_before != count_after:
            raise Exception(f"Row count mismatch! Before={count_before:,}, After={count_after:,}")

        # Drop old table
        api.query(f"DROP TABLE {table_name}")
        logger.info(f"[{table_name}] Dropped old table")

        # Rename new table
        api.query(f"ALTER TABLE {new_table} RENAME TO {table_name}")
        logger.info(f"[{table_name}] ✅ Renamed {new_table} to {table_name}")

        return True

    except Exception as e:
        logger.error(f"[{table_name}] ❌ Failed to recreate table: {e}")
        # Rollback: drop new table if exists
        try:
            api.query(f"DROP TABLE IF EXISTS {new_table}")
            logger.info(f"[{table_name}] Cleaned up temporary table")
        except:
            pass
        raise

def verify_migration(api, table_name):
    """Verify migration success."""
    logger.info(f"[{table_name}] Verifying migration...")

    # Check PRIMARY KEY
    pk_sql = f"""
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_name = '{table_name}'
        AND constraint_type = 'PRIMARY KEY'
    """
    pk = api.query(pk_sql)
    if len(pk) == 0:
        raise Exception(f"No PRIMARY KEY found!")
    logger.info(f"[{table_name}] PRIMARY KEY: {pk['constraint_name'][0]}")

    # Check report_type column exists
    schema = get_table_schema(api, table_name)
    if 'report_type' not in schema['column_name'].values:
        raise Exception(f"report_type column missing!")

    report_type_info = schema[schema['column_name'] == 'report_type'].iloc[0]
    logger.info(f"[{table_name}] report_type column: {report_type_info['data_type']} (nullable={report_type_info['is_nullable']})")

    # Check data integrity
    count = api.query(f"SELECT COUNT(*) as cnt FROM {table_name}")['cnt'][0]
    logger.info(f"[{table_name}] Total rows: {count:,}")

    # Check for NULLs in report_type (should be 0)
    null_count = api.query(f"SELECT COUNT(*) as cnt FROM {table_name} WHERE report_type IS NULL")['cnt'][0]
    if null_count > 0:
        raise Exception(f"Found {null_count:,} NULL report_type values!")

    # Check report_type distribution
    dist = api.query(f"""
        SELECT report_type, COUNT(*) as cnt
        FROM {table_name}
        GROUP BY report_type
        ORDER BY report_type
    """)
    logger.info(f"[{table_name}] report_type distribution:")
    for _, row in dist.iterrows():
        logger.info(f"  - report_type={row['report_type']}: {row['cnt']:,} records")

    logger.info(f"[{table_name}] ✅ Verification passed")
    return True

def main():
    logger.info("="*80)
    logger.info("Migration: Add report_type and update PRIMARY KEY")
    logger.info("="*80)

    api = create_api()
    api.initialize()

    try:
        # Phase 1: Add report_type column
        logger.info("\n" + "="*80)
        logger.info("Phase 1: Adding report_type columns")
        logger.info("="*80)

        for table in TABLES:
            if not add_report_type_column(api, table):
                logger.error(f"Failed to add report_type to {table}")
                return 1

        # Phase 2: Recreate tables with new PRIMARY KEY
        logger.info("\n" + "="*80)
        logger.info("Phase 2: Updating PRIMARY KEY constraints")
        logger.info("="*80)

        for table in TABLES:
            try:
                recreate_table_with_new_pk(api, table)
            except Exception as e:
                logger.error(f"Failed to recreate {table}: {e}")
                return 1

        # Phase 3: Verify
        logger.info("\n" + "="*80)
        logger.info("Phase 3: Verifying migration")
        logger.info("="*80)

        for table in TABLES:
            try:
                verify_migration(api, table)
            except Exception as e:
                logger.error(f"Verification failed for {table}: {e}")
                return 1

        logger.info("\n" + "="*80)
        logger.info("✅ Migration completed successfully!")
        logger.info("="*80)

        logger.info("\nNext steps:")
        logger.info("1. Update duckdb_impl.py: Change 'INSERT OR REPLACE' to 'INSERT OR IGNORE'")
        logger.info("2. Re-fetch data to populate multiple versions")
        logger.info("3. Run validation script to verify 100% pass rate")
        logger.info("4. Consider creating views for 'latest version' queries")

        # Show summary
        logger.info("\nMigration Summary:")
        logger.info("-" * 80)
        for table in TABLES:
            count = api.query(f"SELECT COUNT(*) as cnt FROM {table}")['cnt'][0]
            logger.info(f"{table:25s}: {count:,} records")

        return 0

    except Exception as e:
        logger.error(f"\n❌ Migration FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        api.close()

if __name__ == "__main__":
    sys.exit(main())
