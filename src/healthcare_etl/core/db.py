import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
from healthcare_etl.core.config import DATABASE_URL
from healthcare_etl.models.tables import Base

log = logging.getLogger(__name__)

def get_engine():
    try:
        return create_engine(DATABASE_URL, echo=False, future=True)
    except SQLAlchemyError as e:
        log.error("Failed to create engine: %s", e)
        raise
    
def create_tables():
    """Create missing tables (idempotent)."""
    engine = get_engine()
    insp = inspect(engine)
    existing = set(insp.get_table_names())
    expected = set(Base.metadata.tables.keys())

    if expected.issubset(existing):
        log.info("All tables exist. Skipping creation.")
        return engine

    missing = sorted(expected - existing)
    log.info("Creating tables: %s", ", ".join(missing))
    Base.metadata.create_all(engine)
    log.info("Tables created.")
    return engine