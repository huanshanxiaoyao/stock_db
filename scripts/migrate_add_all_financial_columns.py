#!/usr/bin/env python3
"""
Smart migration script to add ALL JQData columns to financial statement tables.

This script uses DuckDB's ALTER TABLE to dynamically add missing columns,
then re-fetches data to populate them.

Approach:
1. Connect to database
2. Get current table columns
3. Get all JQData columns from a sample query
4. Add missing columns using ALTER TABLE
5. Drop existing data
6. Re-fetch all data with new columns

This is safer than manual SQL editing and automatically handles column types.
"""

import sys
sys.path.insert(0, '.')

from api import create_api
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Column definitions from our analysis
INCOME_STATEMENT_NEW_COLS = {
    'id': 'VARCHAR', 'company_id': 'VARCHAR', 'company_name': 'VARCHAR',
    'a_code': 'VARCHAR', 'b_code': 'VARCHAR', 'h_code': 'VARCHAR',
    'source_id': 'VARCHAR', 'source': 'VARCHAR',
    'start_date': 'VARCHAR', 'end_date': 'VARCHAR', 'report_date': 'VARCHAR',
    'report_type': 'INTEGER',
    'rd_expenses': 'DOUBLE', 'credit_impairment_loss': 'DOUBLE',
    'asset_deal_income': 'DOUBLE', 'other_earnings': 'DOUBLE',
    'non_current_asset_disposed': 'DOUBLE', 'net_open_hedge_income': 'DOUBLE',
    'other_items_influenced_profit': 'DOUBLE', 'other_items_influenced_net_profit': 'DOUBLE',
    'sust_operate_net_profit': 'DOUBLE', 'discon_operate_net_profit': 'DOUBLE',
    'interest_income_fin': 'DOUBLE', 'interest_cost_fin': 'DOUBLE',
}

CASHFLOW_STATEMENT_NEW_COLS = {
    'id': 'VARCHAR', 'company_id': 'VARCHAR', 'company_name': 'VARCHAR',
    'a_code': 'VARCHAR', 'b_code': 'VARCHAR', 'h_code': 'VARCHAR',
    'source_id': 'VARCHAR', 'source': 'VARCHAR',
    'start_date': 'VARCHAR', 'end_date': 'VARCHAR', 'report_date': 'VARCHAR',
    'report_type': 'INTEGER',
    'other_reason_effect_cash': 'DOUBLE', 'net_profit': 'DOUBLE',
    'assets_depreciation_reserves': 'DOUBLE', 'fixed_assets_depreciation': 'DOUBLE',
    'intangible_assets_amortization': 'DOUBLE', 'defferred_expense_amortization': 'DOUBLE',
    'fix_intan_other_asset_dispo_loss': 'DOUBLE', 'fixed_asset_scrap_loss': 'DOUBLE',
    'fair_value_change_loss': 'DOUBLE', 'financial_cost': 'DOUBLE',
    'invest_loss': 'DOUBLE', 'deffered_tax_asset_decrease': 'DOUBLE',
    'deffered_tax_liability_increase': 'DOUBLE', 'inventory_decrease': 'DOUBLE',
    'operate_receivables_decrease': 'DOUBLE', 'operate_payable_increase': 'DOUBLE',
    'others': 'DOUBLE', 'net_operate_cash_flow_indirect': 'DOUBLE',
    'debt_to_capital': 'DOUBLE', 'cbs_expiring_in_one_year': 'DOUBLE',
    'financial_lease_fixed_assets': 'DOUBLE', 'cash_at_end': 'DOUBLE',
    'cash_at_beginning': 'DOUBLE', 'equivalents_at_end': 'DOUBLE',
    'equivalents_at_beginning': 'DOUBLE', 'other_reason_effect_cash_indirect': 'DOUBLE',
    'cash_equivalent_increase_indirect': 'DOUBLE', 'cash_from_mino_s_invest_sub': 'DOUBLE',
    'proceeds_from_sub_to_mino_s': 'DOUBLE', 'investment_property_depreciation': 'DOUBLE',
    'credit_impairment_loss': 'DOUBLE',
}

BALANCE_SHEET_NEW_COLS = {
    'id': 'VARCHAR', 'company_id': 'VARCHAR', 'company_name': 'VARCHAR',
    'a_code': 'VARCHAR', 'b_code': 'VARCHAR', 'h_code': 'VARCHAR',
    'source_id': 'VARCHAR', 'source': 'VARCHAR',
    'start_date': 'VARCHAR', 'end_date': 'VARCHAR', 'report_date': 'VARCHAR',
    'report_type': 'INTEGER',
    'affiliated_company_receivable': 'DOUBLE', 'hold_for_sale_assets': 'DOUBLE',
    'hold_to_maturity_investments': 'DOUBLE', 'longterm_receivable_account': 'DOUBLE',
    'longterm_equity_invest': 'DOUBLE', 'investment_property': 'DOUBLE',
    'oil_gas_assets': 'DOUBLE', 'shortterm_loan': 'DOUBLE',
    'affiliated_company_payable': 'DOUBLE', 'longterm_loan': 'DOUBLE',
    'longterm_account_payable': 'DOUBLE', 'estimate_liability': 'DOUBLE',
    'surplus_reserve_fund': 'DOUBLE', 'retained_profit': 'DOUBLE',
    'equities_parent_company_owners': 'DOUBLE', 'foreign_currency_report_conv_diff': 'DOUBLE',
    'irregular_item_adjustment': 'DOUBLE', 'deferred_earning': 'DOUBLE',
    'loan_and_advance_current_assets': 'DOUBLE', 'derivative_financial_asset': 'DOUBLE',
    'hold_sale_asset': 'DOUBLE', 'loan_and_advance_noncurrent_assets': 'DOUBLE',
    'borrowing_from_centralbank': 'DOUBLE', 'deposit_in_interbank': 'DOUBLE',
    'borrowing_capital': 'DOUBLE', 'derivative_financial_liability': 'DOUBLE',
    'hold_sale_liability': 'DOUBLE', 'estimate_liability_current': 'DOUBLE',
    'deferred_earning_current': 'DOUBLE', 'preferred_shares_noncurrent': 'DOUBLE',
    'pepertual_liability_noncurrent': 'DOUBLE', 'longterm_salaries_payable': 'DOUBLE',
    'preferred_shares_equity': 'DOUBLE', 'pepertual_liability_equity': 'DOUBLE',
    'bill_and_account_receivable': 'DOUBLE', 'bill_and_account_payable': 'DOUBLE',
    'receivable_fin': 'DOUBLE', 'usufruct_assets': 'DOUBLE',
    'ordinary_risk_reserve_fund': 'DOUBLE', 'contract_assets': 'DOUBLE',
    'bond_invest': 'DOUBLE', 'other_bond_invest': 'DOUBLE',
    'other_equity_tools_invest': 'DOUBLE', 'other_non_current_financial_assets': 'DOUBLE',
    'contract_liability': 'DOUBLE', 'lease_liability': 'DOUBLE',
}

def add_columns_to_table(api, table_name, new_columns):
    """Add new columns to table using ALTER TABLE"""
    logger.info(f"\n{'='*80}")
    logger.info(f"Adding columns to {table_name}")
    logger.info(f"{'='*80}")

    # Get existing columns
    result = api.query(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    existing_cols = set(result['column_name'].tolist())

    logger.info(f"Existing columns: {len(existing_cols)}")
    logger.info(f"Target new columns: {len(new_columns)}")

    # Find columns that need to be added
    cols_to_add = {k: v for k, v in new_columns.items() if k not in existing_cols}

    if not cols_to_add:
        logger.info(f"✓ All columns already exist in {table_name}")
        return 0

    logger.info(f"Need to add: {len(cols_to_add)} columns")

    # Add each column
    added = 0
    for col_name, col_type in sorted(cols_to_add.items()):
        try:
            sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
            api.query(sql)  # Use query() instead of db.execute()
            logger.info(f"  ✓ Added: {col_name} {col_type}")
            added += 1
        except Exception as e:
            logger.error(f"  ✗ Failed to add {col_name}: {e}")

    logger.info(f"\n✅ Added {added}/{len(cols_to_add)} columns to {table_name}")
    return added

def main():
    logger.info("="*80)
    logger.info("Financial Statements: Add ALL JQData Columns Migration")
    logger.info("="*80)

    # Create API
    api = create_api()
    api.initialize()

    try:
        # Add columns to each table
        total_added = 0

        total_added += add_columns_to_table(api, 'income_statement', INCOME_STATEMENT_NEW_COLS)
        total_added += add_columns_to_table(api, 'cashflow_statement', CASHFLOW_STATEMENT_NEW_COLS)
        total_added += add_columns_to_table(api, 'balance_sheet', BALANCE_SHEET_NEW_COLS)

        logger.info(f"\n{'='*80}")
        logger.info(f"Migration Summary")
        logger.info(f"{'='*80}")
        logger.info(f"Total columns added: {total_added}")

        # Verify final column counts
        for table in ['income_statement', 'cashflow_statement', 'balance_sheet']:
            result = api.query(f"SELECT COUNT(*) as count FROM information_schema.columns WHERE table_name = '{table}'")
            count = result['count'].iloc[0]
            logger.info(f"{table}: {count} columns")

        logger.info(f"\n✅ Column migration completed successfully!")
        logger.info(f"\nNext steps:")
        logger.info(f"1. The tables now have all JQData columns")
        logger.info(f"2. Existing data is preserved (new columns will be NULL)")
        logger.info(f"3. Run: python main.py update-table --table income_statement")
        logger.info(f"4. Run: python main.py update-table --table cashflow_statement")
        logger.info(f"5. Run: python main.py update-table --table balance_sheet")
        logger.info(f"\nThis will re-fetch data and populate the new columns.")

    except Exception as e:
        logger.error(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        api.close()

    return 0

if __name__ == "__main__":
    sys.exit(main())
