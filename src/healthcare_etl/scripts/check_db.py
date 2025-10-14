"""
Check the database connection and list tables.
Run with: python -m healthcare_etl.scripts.check_db
"""
from sqlalchemy import inspect,text
from healthcare_etl.core.db import get_engine

def main():
    try:
        engine = get_engine()
        insp = inspect(engine)
        
        print("Database Connection: SUCCESS\n")
        print("Tables in database:")
        
        tables = insp.get_table_names()
        if tables:
            for table in tables:
                with engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.fetchone()[0]
                    print(f"  - {table}: {count} rows")
        else:
            print("  No tables found")
            
    except Exception as e:
        print(f"Database Connection: FAILED")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()