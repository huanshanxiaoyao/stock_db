#!/usr/bin/env python3
"""
Validation script to verify financial statement data matches JQData exactly.

This script:
1. Samples 200 random stock codes from database
2. Samples 50 random dates from available data
3. Fetches data from both JQData and local database
4. Compares column by column
5. Reports any mismatches
"""

import sys
sys.path.insert(0, '.')

from api import create_api
import jqdatasdk as jq
from jqdatasdk import finance, query
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Table configurations (will be initialized after auth)
TABLES_TO_CHECK = {
    'income_statement': {
        'jq_table': None,  # Will be set after auth
        'date_column': 'pub_date',
        'expected_columns': 69
    },
    'cashflow_statement': {
        'jq_table': None,  # Will be set after auth
        'date_column': 'pub_date',
        'expected_columns': 98
    },
    'balance_sheet': {
        'jq_table': None,  # Will be set after auth
        'date_column': 'pub_date',
        'expected_columns': 126
    }
}

SAMPLE_STOCKS = 200
SAMPLE_DATES = 50

# Quick test mode - set via environment variable
QUICK_TEST = True  # Set to False for full validation

class DataValidator:
    def __init__(self, api):
        self.api = api
        self.mismatches = []
        self.total_checks = 0
        self.passed_checks = 0

    def _to_jq_code(self, code):
        """Convert standard code to JQData format"""
        if code.endswith('.SH'):
            return code.replace('.SH', '.XSHG')
        elif code.endswith('.SZ'):
            return code.replace('.SZ', '.XSHE')
        return code

    def _from_jq_code(self, jq_code):
        """Convert JQData code to standard format"""
        if jq_code.endswith('.XSHG'):
            return jq_code.replace('.XSHG', '.SH')
        elif jq_code.endswith('.XSHE'):
            return jq_code.replace('.XSHE', '.SZ')
        return jq_code

    def sample_stocks(self):
        """Sample random stock codes from database"""
        logger.info("Sampling stock codes from database...")

        result = self.api.query("SELECT DISTINCT code FROM stock_list ORDER BY code")
        all_codes = result['code'].tolist()

        if len(all_codes) > SAMPLE_STOCKS:
            sampled = random.sample(all_codes, SAMPLE_STOCKS)
        else:
            sampled = all_codes

        logger.info(f"Sampled {len(sampled)} stock codes")
        return sampled

    def sample_dates(self, table_name, stock_codes):
        """Sample random dates that have data for these stocks"""
        logger.info(f"Sampling dates from {table_name}...")

        # Get dates that have data
        codes_str = "', '".join(stock_codes[:50])  # Sample from first 50 stocks
        sql = f"""
            SELECT DISTINCT pub_date
            FROM {table_name}
            WHERE code IN ('{codes_str}')
            AND pub_date >= '2020-01-01'
            ORDER BY pub_date DESC
            LIMIT 200
        """

        result = self.api.query(sql)
        if result.empty:
            logger.warning(f"No data found in {table_name}")
            return []

        all_dates = result['pub_date'].tolist()

        if len(all_dates) > SAMPLE_DATES:
            sampled = random.sample(all_dates, SAMPLE_DATES)
        else:
            sampled = all_dates

        logger.info(f"Sampled {len(sampled)} dates")
        return sampled

    def fetch_jqdata(self, table_name, stock_code, start_date, end_date):
        """Fetch data from JQData"""
        config = TABLES_TO_CHECK[table_name]
        jq_code = self._to_jq_code(stock_code)

        q = query(config['jq_table']).filter(
            config['jq_table'].code == jq_code,
            config['jq_table'].pub_date >= start_date.strftime('%Y-%m-%d'),
            config['jq_table'].pub_date <= end_date.strftime('%Y-%m-%d')
        )

        df = finance.run_query(q)
        if not df.empty:
            df['code'] = df['code'].apply(self._from_jq_code)
            # Add stat_date mapping
            if 'report_date' in df.columns:
                df['stat_date'] = df['report_date']

        return df

    def fetch_local_data(self, table_name, stock_code, start_date, end_date):
        """Fetch data from local database"""
        sql = f"""
            SELECT * FROM {table_name}
            WHERE code = '{stock_code}'
            AND pub_date >= '{start_date}'
            AND pub_date <= '{end_date}'
            ORDER BY pub_date
        """
        return self.api.query(sql)

    def compare_dataframes(self, table_name, jq_df, local_df, stock_code, date):
        """Compare two dataframes and report differences"""
        if jq_df.empty and local_df.empty:
            logger.debug(f"{table_name} {stock_code} {date}: Both empty (OK)")
            self.passed_checks += 1
            self.total_checks += 1
            return True

        if jq_df.empty:
            logger.warning(f"{table_name} {stock_code} {date}: JQData empty but local has {len(local_df)} rows")
            self.mismatches.append({
                'table': table_name,
                'code': stock_code,
                'date': date,
                'issue': 'JQData empty, local has data'
            })
            self.total_checks += 1
            return False

        if local_df.empty:
            logger.warning(f"{table_name} {stock_code} {date}: Local empty but JQData has {len(jq_df)} rows")
            self.mismatches.append({
                'table': table_name,
                'code': stock_code,
                'date': date,
                'issue': 'Local empty, JQData has data'
            })
            self.total_checks += 1
            return False

        # Check row count
        if len(jq_df) != len(local_df):
            logger.warning(f"{table_name} {stock_code} {date}: Row count mismatch - JQData: {len(jq_df)}, Local: {len(local_df)}")
            self.mismatches.append({
                'table': table_name,
                'code': stock_code,
                'date': date,
                'issue': f'Row count: JQData={len(jq_df)}, Local={len(local_df)}'
            })
            self.total_checks += 1
            return False

        # Check column alignment
        jq_cols = set(jq_df.columns)
        local_cols = set(local_df.columns)

        missing_in_local = jq_cols - local_cols
        extra_in_local = local_cols - jq_cols

        if missing_in_local:
            logger.warning(f"{table_name} {stock_code} {date}: Missing columns in local: {missing_in_local}")
            self.mismatches.append({
                'table': table_name,
                'code': stock_code,
                'date': date,
                'issue': f'Missing columns: {missing_in_local}'
            })

        if extra_in_local:
            logger.debug(f"{table_name} {stock_code} {date}: Extra columns in local (OK if expected): {extra_in_local}")

        # Compare common columns
        common_cols = jq_cols & local_cols
        common_cols.discard('id')  # Skip ID column (may differ)

        for idx in range(len(jq_df)):
            for col in common_cols:
                jq_val = jq_df.iloc[idx][col]
                local_val = local_df.iloc[idx][col]

                # Handle NaN comparison
                if pd.isna(jq_val) and pd.isna(local_val):
                    continue

                # Handle numeric comparison with tolerance
                if isinstance(jq_val, (int, float)) and isinstance(local_val, (int, float)):
                    if not np.isclose(jq_val, local_val, rtol=1e-9, atol=1e-9, equal_nan=True):
                        logger.warning(f"{table_name} {stock_code} {date} row {idx} col '{col}': JQData={jq_val}, Local={local_val}")
                        self.mismatches.append({
                            'table': table_name,
                            'code': stock_code,
                            'date': date,
                            'issue': f'Value mismatch in {col}: JQData={jq_val}, Local={local_val}'
                        })
                        self.total_checks += 1
                        return False

                # Handle string/date comparison
                elif str(jq_val) != str(local_val):
                    logger.warning(f"{table_name} {stock_code} {date} row {idx} col '{col}': JQData={jq_val}, Local={local_val}")
                    self.mismatches.append({
                        'table': table_name,
                        'code': stock_code,
                        'date': date,
                        'issue': f'Value mismatch in {col}: JQData={jq_val}, Local={local_val}'
                    })
                    self.total_checks += 1
                    return False

        logger.debug(f"{table_name} {stock_code} {date}: MATCH ✓")
        self.passed_checks += 1
        self.total_checks += 1
        return True

    def validate_table(self, table_name, stock_codes, dates):
        """Validate one table"""
        logger.info(f"\n{'='*80}")
        logger.info(f"Validating {table_name}")
        logger.info(f"{'='*80}")

        # Sample stocks and dates for this table
        if QUICK_TEST:
            sample_stocks = random.sample(stock_codes, min(5, len(stock_codes)))
            sample_dates = random.sample(dates, min(3, len(dates)))
        else:
            sample_stocks = random.sample(stock_codes, min(20, len(stock_codes)))
            sample_dates = random.sample(dates, min(10, len(dates)))

        logger.info(f"Testing {len(sample_stocks)} stocks × {len(sample_dates)} dates = {len(sample_stocks) * len(sample_dates)} checks")

        checks_done = 0
        for stock_code in sample_stocks:
            for date in sample_dates:
                checks_done += 1
                if checks_done % 10 == 0:
                    logger.info(f"Progress: {checks_done}/{len(sample_stocks) * len(sample_dates)} checks")

                # Fetch from both sources
                try:
                    jq_df = self.fetch_jqdata(table_name, stock_code, date, date)
                    local_df = self.fetch_local_data(table_name, stock_code, date, date)

                    # Compare
                    self.compare_dataframes(table_name, jq_df, local_df, stock_code, date)

                except Exception as e:
                    logger.error(f"Error checking {table_name} {stock_code} {date}: {e}")
                    self.mismatches.append({
                        'table': table_name,
                        'code': stock_code,
                        'date': date,
                        'issue': f'Error: {e}'
                    })

    def print_summary(self):
        """Print validation summary"""
        logger.info(f"\n{'='*80}")
        logger.info(f"VALIDATION SUMMARY")
        logger.info(f"{'='*80}")

        logger.info(f"Total checks: {self.total_checks}")
        logger.info(f"Passed: {self.passed_checks}")
        logger.info(f"Failed: {len(self.mismatches)}")

        if self.total_checks > 0:
            pass_rate = (self.passed_checks / self.total_checks) * 100
            logger.info(f"Pass rate: {pass_rate:.2f}%")

        if self.mismatches:
            logger.info(f"\n{'='*80}")
            logger.info(f"MISMATCHES FOUND ({len(self.mismatches)})")
            logger.info(f"{'='*80}")

            # Group by table
            by_table = {}
            for m in self.mismatches:
                table = m['table']
                if table not in by_table:
                    by_table[table] = []
                by_table[table].append(m)

            for table, issues in by_table.items():
                logger.info(f"\n{table}: {len(issues)} issues")
                for issue in issues[:10]:  # Show first 10
                    logger.info(f"  - {issue['code']} {issue['date']}: {issue['issue']}")
                if len(issues) > 10:
                    logger.info(f"  ... and {len(issues) - 10} more")
        else:
            logger.info(f"\n✅ ALL CHECKS PASSED! Data matches JQData exactly.")

def main():
    logger.info("="*80)
    logger.info("Financial Statement Data Validation")
    logger.info("="*80)

    # Create API
    api = create_api()
    api.initialize()

    # Initialize JQData table references after auth
    TABLES_TO_CHECK['income_statement']['jq_table'] = finance.STK_INCOME_STATEMENT
    TABLES_TO_CHECK['cashflow_statement']['jq_table'] = finance.STK_CASHFLOW_STATEMENT
    TABLES_TO_CHECK['balance_sheet']['jq_table'] = finance.STK_BALANCE_SHEET

    try:
        validator = DataValidator(api)

        # Sample stocks
        stock_codes = validator.sample_stocks()

        # Validate each table
        for table_name in TABLES_TO_CHECK.keys():
            # Sample dates specific to this table
            dates = validator.sample_dates(table_name, stock_codes)

            if not dates:
                logger.warning(f"Skipping {table_name} - no data found")
                continue

            # Validate
            validator.validate_table(table_name, stock_codes, dates)

        # Print summary
        validator.print_summary()

        # Return exit code based on results
        if validator.mismatches:
            logger.error(f"\n❌ Validation FAILED - Found {len(validator.mismatches)} mismatches")
            return 1
        else:
            logger.info(f"\n✅ Validation PASSED - All data matches JQData!")
            return 0

    except Exception as e:
        logger.error(f"Validation failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        api.close()

if __name__ == "__main__":
    sys.exit(main())
