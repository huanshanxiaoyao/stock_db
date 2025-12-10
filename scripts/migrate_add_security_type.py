"""Migration: Add security_type field to stock_list table

This allows storing both stocks and indices in the same table.
"""

from database import DatabaseManager
from duckdb_impl import DuckDBDatabase
from config import get_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def migrate():
    """Add security_type field to existing stock_list table"""
    config = get_config()
    db = DuckDBDatabase(config.database.path)

    logger.info("=" * 60)
    logger.info("Migration: Adding security_type field to stock_list")
    logger.info("=" * 60)

    try:
        # Connect to database
        db.connect()

        # Check if column already exists
        result = db.query_data("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'stock_list'
            AND column_name = 'security_type'
        """)

        if not result.empty:
            logger.info("✓ security_type column already exists")
        else:
            # Add security_type column with default 'stock'
            logger.info("Adding security_type column...")
            db.query_data("ALTER TABLE stock_list ADD COLUMN security_type VARCHAR DEFAULT 'stock'")
            logger.info("✓ Added security_type column")

            # Update existing records to have security_type = 'stock'
            logger.info("Setting security_type = 'stock' for existing records...")
            db.query_data("UPDATE stock_list SET security_type = 'stock' WHERE security_type IS NULL")
            logger.info("✓ Updated existing records")

        # Create index for fast filtering
        logger.info("Creating index on security_type...")
        db.query_data("CREATE INDEX IF NOT EXISTS idx_security_type ON stock_list(security_type)")
        logger.info("✓ Created index idx_security_type")

        # Create combined index for common query pattern
        logger.info("Creating composite index on (exchange, security_type)...")
        db.query_data("CREATE INDEX IF NOT EXISTS idx_exchange_security_type ON stock_list(exchange, security_type)")
        logger.info("✓ Created index idx_exchange_security_type")

        # Optional: Create views for convenience
        logger.info("Creating convenience views...")
        db.query_data("""
            CREATE OR REPLACE VIEW stocks AS
            SELECT * FROM stock_list WHERE security_type = 'stock'
        """)
        db.query_data("""
            CREATE OR REPLACE VIEW indices AS
            SELECT * FROM stock_list WHERE security_type = 'index'
        """)
        logger.info("✓ Created views: stocks, indices")

        # Verify migration
        logger.info("\nVerifying migration...")
        stock_count = db.query_data("SELECT COUNT(*) as cnt FROM stock_list WHERE security_type = 'stock'")
        index_count = db.query_data("SELECT COUNT(*) as cnt FROM stock_list WHERE security_type = 'index'")
        total_count = db.query_data("SELECT COUNT(*) as cnt FROM stock_list")

        logger.info("=" * 60)
        logger.info("Migration Summary:")
        logger.info("=" * 60)
        logger.info(f"  Total securities: {total_count['cnt'][0]}")
        logger.info(f"  Stocks: {stock_count['cnt'][0]}")
        logger.info(f"  Indices: {index_count['cnt'][0]}")
        logger.info("=" * 60)
        logger.info("✅ Migration completed successfully!")
        logger.info("=" * 60)

        db.close()
        return True

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"❌ Migration failed: {e}")
        logger.error("=" * 60)
        if db and db.conn:
            db.close()
        raise


if __name__ == '__main__':
    migrate()
