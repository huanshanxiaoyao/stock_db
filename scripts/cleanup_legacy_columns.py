#!/usr/bin/env python3
"""
Cleanup script to remove LEGACY columns that JQData no longer provides.

JQData changed some column names over time. This script removes the old columns
that are no longer returned by the API.
"""

import sys
sys.path.insert(0, '.')

from api import create_api
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Legacy columns that need to be removed (JQData no longer provides these)
BALANCE_SHEET_LEGACY_COLS = [
    'loan_and_advance_current',      # Now: loan_and_advance_current_assets
    'available_for_sale_assets',      # Removed by JQData
    'held_to_maturity_investments',   # Now: hold_to_maturity_investments (typo fix)
    'long_term_receivable',           # Now: longterm_receivable_account
    'long_term_equity_invest',        # Now: longterm_equity_invest
    'investment_real_estate',         # Now: investment_property
    'oil_and_gas_assets',             # Now: oil_gas_assets
    'short_term_loan',                # Now: shortterm_loan
    'borrowing_from_central_bank',    # Now: borrowing_from_centralbank
    'lend_capital_liability',         # Removed by JQData
    'long_term_loan',                 # Now: longterm_loan
    'long_term_account_payable',      # Now: longterm_account_payable
    'estimated_liability',            # Now: estimate_liability
    'other_equity_tools_PRE_STOCK',   # Removed by JQData
    'other_equity_tools_PERPETUAL_DEBT', # Removed by JQData
    'other_equity_tools_OTHER',       # Removed by JQData
    'earned_surplus',                 # Now: surplus_reserve_fund
    'general_risk_preparation',       # Now: ordinary_risk_reserve_fund
    'undistributed_profit',           # Now: retained_profit
    'foreign_exchange_gain',          # Now: foreign_currency_report_conv_diff
    'total_liability_equity',         # Removed by JQData (calculated field)
]

def remove_legacy_columns(api, table_name, legacy_columns):
    """Remove legacy columns from table"""
    logger.info(f"\n{'='*80}")
    logger.info(f"Cleaning up legacy columns in {table_name}")
    logger.info(f"{'='*80}")

    # Get existing columns
    result = api.query(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    existing_cols = set(result['column_name'].tolist())

    logger.info(f"Table currently has {len(existing_cols)} columns")

    # Find legacy columns that exist
    cols_to_remove = [col for col in legacy_columns if col in existing_cols]

    if not cols_to_remove:
        logger.info(f"✓ No legacy columns found in {table_name}")
        return 0

    logger.info(f"Found {len(cols_to_remove)} legacy columns to remove:")
    for col in cols_to_remove:
        logger.info(f"  - {col}")

    # Remove each column
    removed = 0
    for col_name in sorted(cols_to_remove):
        try:
            sql = f"ALTER TABLE {table_name} DROP COLUMN {col_name}"
            api.query(sql)
            logger.info(f"  ✓ Removed: {col_name}")
            removed += 1
        except Exception as e:
            logger.error(f"  ✗ Failed to remove {col_name}: {e}")

    logger.info(f"\n✅ Removed {removed}/{len(cols_to_remove)} legacy columns from {table_name}")
    return removed

def main():
    logger.info("=" * 80)
    logger.info("Financial Statements: Remove Legacy Columns")
    logger.info("=" * 80)

    api = create_api()
    api.initialize()

    try:
        # Only balance_sheet has legacy columns
        total_removed = remove_legacy_columns(api, 'balance_sheet', BALANCE_SHEET_LEGACY_COLS)

        logger.info(f"\n{'='*80}")
        logger.info(f"Cleanup Summary")
        logger.info(f"{'='*80}")
        logger.info(f"Total legacy columns removed: {total_removed}")

        # Verify final column count
        result = api.query("SELECT COUNT(*) as count FROM information_schema.columns WHERE table_name = 'balance_sheet'")
        count = result['count'].iloc[0]
        logger.info(f"balance_sheet now has: {count} columns (expected: 126)")

        if count == 126:
            logger.info(f"\n✅ Cleanup completed successfully!")
            logger.info(f"\nNow balance_sheet matches JQData exactly:")
            logger.info(f"  - JQData provides: 126 columns")
            logger.info(f"  - Table has: 126 columns")
            logger.info(f"  - No more warnings! ✓")
        else:
            logger.warning(f"\n⚠️  Expected 126 columns, found {count}")
            logger.warning(f"There may be other legacy columns to remove.")

    except Exception as e:
        logger.error(f"\n❌ Cleanup failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        api.close()

    return 0

if __name__ == "__main__":
    sys.exit(main())
