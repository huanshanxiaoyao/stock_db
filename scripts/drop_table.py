import duckdb

  # Connect to your database
conn = duckdb.connect('stock_data.duckdb')

  # Drop the table
conn.execute("DROP TABLE IF EXISTS mtss_data")

  # Verify
result = conn.execute("SHOW TABLES").fetchall()
print("Remaining tables:", result)

  # Close connection
conn.close()

print("Table mtss_data has been dropped successfully!")
