"""
ORM models for the ETL database.
"""

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import (
    Column, String, Float, Boolean, DateTime, Date, Integer, ForeignKey, Text
)

class Base(DeclarativeBase):
    pass

class Patient(Base):
    __tablename__ = "patients"

    patient_id  = Column(String(20), primary_key=True)
    given_name  = Column(String(100))
    family_name = Column(String(100))
    sex         = Column(String(1))       
    dob         = Column(Date)            
    height      = Column(Float)           
    weight      = Column(Float)             
    qa_flags    = Column(Text)            
    source_file = Column(String(255))

class Encounter(Base):
    __tablename__ = "encounters"

    encounter_id     = Column(String(20), primary_key=True)
    patient_id       = Column(String(20), ForeignKey("patients.patient_id"), nullable=False)
    admit_dt         = Column(DateTime(timezone=True))
    discharge_dt     = Column(DateTime(timezone=True))
    encounter_type   = Column(String(50))  
    encounter_status = Column(String(20))   
    qa_flags         = Column(Text)
    source_file      = Column(String(255))

class Diagnosis(Base):
    __tablename__ = "diagnoses"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    encounter_id   = Column(String(20), ForeignKey("encounters.encounter_id"), nullable=False)
    code_system    = Column(String(50))   
    diagnosis_code = Column(String(50))    
    is_primary     = Column(Boolean)
    recorded_at    = Column(DateTime(timezone=True))
    qa_flags       = Column(Text)
    source_file    = Column(String(255))