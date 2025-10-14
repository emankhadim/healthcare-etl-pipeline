# Healthcare ETL Pipeline

A Python-based ETL pipeline that extracts healthcare data from CSV/XML files, transforms and validates it, and loads clean data into PostgreSQL with comprehensive data quality controls.

---

## Overview

This system:
1. **Extracts** patient, encounter, and diagnosis data from raw files
2. **Transforms** data with validation, deduplication, and referential integrity checks
3. **Loads** clean data into PostgreSQL database
4. **Monitors** data quality with an interactive dashboard

---

## Project Structure
```
healthcare-etl-pipeline/
├── src/healthcare_etl/
│   ├── core/              # Configuration and database setup
│   ├── extract/           # Data extraction from CSV/XML
│   ├── transform/         # Data cleaning and validation
│   ├── load/              # Database loading
│   ├── models/            # SQLAlchemy ORM models
│   ├── services/          # ETL orchestration
│   └── scripts/           # Runnable scripts
├── dashboard/             # Streamlit web dashboard
├── data/
│   ├── raw/              # Input files
│   ├── cleaned/          # Processed files
│   └── logs/             # Data quality logs
├── tests/                # Unit tests
├── docker-compose.yml    # PostgreSQL + Adminer setup
├── requirements.txt      # Python dependencies
└── setup.py             # Package installation
```

---

## Features

- **Multi-format Support** - Handles CSV and XML input files
- **Data Validation** - Type checking, format standardization, date logic validation
- **Referential Integrity** - Foreign key validation before database load
- **Duplicate Detection** - Automatic deduplication with survivorship rules
- **Comprehensive Logging** - All data quality issues tracked with flags
- **Interactive Dashboard** - Real-time data distribution and quality monitoring
- **Docker Support** - One-command database setup

---

## Data Model

| Table | Description |
|-------|-------------|
| **patients** | Patient demographic information |
| **encounters** | Hospital visits and appointments |
| **diagnoses** | Medical diagnoses linked to encounters |

![Database Schema](data-model.png)

The database consists of three main tables with referential integrity:

**Relationships:**
- One patient can have multiple encounters (1:N)
- One encounter can have multiple diagnoses (1:N)

**Key Points:**
- `patient_id` links encounters to patients
- `encounter_id` links diagnoses to encounters
- Foreign key constraints ensure data integrity
- `qa_flags` track data quality issues in each table
---

## Getting Started

### Prerequisites

- Python 3.10+
- Docker and Docker Compose
- Git

### Installation
```bash
# 1. Clone repository
git clone <your-repo-url>
cd healthcare-etl-pipeline

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install package and dependencies
pip install -e .

# 4. Setup environment variables
cp .env.example .env
# Edit .env if needed (default values work with docker-compose)

# 5. Start PostgreSQL database
docker-compose up -d

# 6. Run ETL pipeline
python -m healthcare_etl.scripts.run_etl

# 7. Launch dashboard
streamlit run dashboard/app.py
```

**Dashboard URL:** http://localhost:8501  
**Adminer (Database UI):** http://localhost:8090

---

## Usage

### Run ETL Pipeline
```bash
python -m healthcare_etl.scripts.run_etl
```

### Check Database Connection
```bash
python -m healthcare_etl.scripts.check_db
```

### View Dashboard
```bash
streamlit run dashboard/app.py
```

### Run Tests
```bash
pytest tests/ -v
```

---

## Data Quality Controls

The pipeline implements several data quality checks:

**Patients:**
- Duplicate detection on patient_id
- Date of birth validation
- Gender standardization

**Encounters:**
- Date logic validation (discharge >= admit)
- Patient foreign key validation
- Encounter type standardization
- Status tracking (OPEN/CLOSED)

**Diagnoses:**
- ICD-10 code format validation
- Encounter foreign key validation
- Primary diagnosis flagging

All rejected records are logged to `data/logs/` with detailed quality flags.

---

## Configuration

Database settings in `.env`:
```env
DB_USER=etl_user
DB_PASSWORD=etl_pass
DB_HOST=localhost
DB_PORT=5433
DB_NAME=healthcare_db
```

---

## Dashboard Features

The interactive dashboard provides:

- **Data Summary** - Record counts and quality scores
- **Patient Demographics** - Gender and age distribution
- **Encounter Distribution** - Visit types and status breakdown
- **Data Quality Report** - Detailed issue tracking with explanations
- **Data Explorer** - Browse and filter loaded data

---

## Development

### Project Dependencies

- pandas - Data manipulation
- SQLAlchemy - Database ORM
- psycopg2-binary - PostgreSQL driver
- streamlit - Dashboard framework
- plotly - Interactive charts
- pytest - Testing framework

### Adding Sample Data

Place your input files in `data/raw/`:
- `patients.csv`
- `encounters.csv`
- `diagnoses.xml`

Run the ETL to process them.

---

## Troubleshooting

**Database Connection Failed**
```bash
# Check if PostgreSQL is running
docker ps

# Restart database
docker-compose down
docker-compose up -d
```

**Import Errors**
```bash
# Reinstall package
pip install -e .
```

**Port Already in Use**
```bash
# Change port in docker-compose.yml
ports:
  - "5434:5432"  # Use different host port

# Update .env
DB_PORT=5434
```

---

## Architecture Decisions

**Why Separate Transform and Load?**
- Data quality issues caught before database operations
- Failed validations don't trigger database rollbacks
- Clean separation of concerns
- Better logging and debugging

**Why Log Rejected Records?**
- Complete audit trail for data quality
- Enables root cause analysis
- Supports data source improvements

**Why Docker?**
- Consistent development environment
- Easy setup for reviewers
- Production-ready deployment

---

## Author

Developed by **Eman Khadim**  
October 2025

--
