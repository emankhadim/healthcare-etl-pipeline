"""
Demo queries - examples of querying the database
Run: python -m healthcare_etl.scripts.run_etl
"""
from healthcare_etl.core.db import get_engine
from sqlalchemy import text

def main():
    engine = get_engine()
    print("Healthcare Database - Sample Queries\n")
    with engine.connect() as conn:

        print("\n1. Patient Count by Gender:")
        result = conn.execute(text("""
            SELECT sex, COUNT(*) as count 
            FROM patients 
            GROUP BY sex
        """))
        for row in result:
            print(f"   {row.sex}: {row.count}")

        print("\n2. Encounter Types:")
        result = conn.execute(text("""
            SELECT encounter_type, COUNT(*) as count 
            FROM encounters 
            GROUP BY encounter_type 
            ORDER BY count DESC
        """))
        for row in result:
            print(f"   {row.encounter_type}: {row.count}")
        
        print("\n3. Top 3 Patients by Encounter Count:")
        result = conn.execute(text("""
            SELECT p.patient_id, p.given_name, p.family_name, COUNT(e.encounter_id) as encounter_count
            FROM patients p
            LEFT JOIN encounters e ON p.patient_id = e.patient_id
            GROUP BY p.patient_id, p.given_name, p.family_name
            ORDER BY encounter_count DESC
            LIMIT 3
        """))
        for row in result:
            print(f"   {row.given_name} {row.family_name} ({row.patient_id}): {row.encounter_count} encounters")
        
        print("\n4. Encounter Status:")
        result = conn.execute(text("""
            SELECT encounter_status, COUNT(*) as count 
            FROM encounters 
            GROUP BY encounter_status
        """))
        for row in result:
            print(f"   {row.encounter_status}: {row.count}")

if __name__ == "__main__":
    main()