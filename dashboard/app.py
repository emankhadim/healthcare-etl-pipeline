"""
Healthcare ETL Dashboard
Interactive visualization of data distribution and quality metrics
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="ETL Dashboard", layout="wide")

# Database connection
@st.cache_resource
def get_connection():
    db_user = os.getenv("DB_USER", "etl_user")
    db_pass = os.getenv("DB_PASSWORD", "etl_pass")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5433")
    db_name = os.getenv("DB_NAME", "healthcare_db")
    
    try:
        conn_str = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
        return create_engine(conn_str)
    except Exception as e:
        st.error(f"Cannot connect to database: {e}")
        return None

@st.cache_data(ttl=60)
def load_table(_engine, table_name):
    try:
        return pd.read_sql(f"SELECT * FROM {table_name}", _engine)
    except:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_log(filepath):
    try:
        if Path(filepath).exists():
            return pd.read_csv(filepath)
    except:
        pass
    return pd.DataFrame()

# Main app
st.title("Healthcare ETL Dashboard")
st.markdown("Monitor data pipeline results and identify quality issues")
st.markdown("---")

engine = get_connection()
if not engine:
    st.stop()

# Load data
patients = load_table(engine, "patients")
encounters = load_table(engine, "encounters")
diagnoses = load_table(engine, "diagnoses")

patients_log = load_log("data/logs/patients_logs.csv")
encounters_log = load_log("data/logs/encounters_logs.csv")
diagnoses_log = load_log("data/logs/diagnoses_logs.csv")

# Summary metrics
st.subheader("Data Summary")
st.markdown("**Overview of successfully loaded records in the database**")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Patients", len(patients))
col1.caption("Unique patient records")

col2.metric("Encounters", len(encounters))
col2.caption("Hospital visits/appointments")

col3.metric("Diagnoses", len(diagnoses))
col3.caption("Medical diagnoses recorded")

total_loaded = len(patients) + len(encounters) + len(diagnoses)
total_rejected = len(patients_log) + len(encounters_log) + len(diagnoses_log)
quality_pct = (total_loaded / (total_loaded + total_rejected) * 100) if (total_loaded + total_rejected) > 0 else 100

col4.metric("Quality Score", f"{quality_pct:.1f}%")
col4.caption(f"{total_loaded} loaded / {total_rejected} rejected")

st.info(f"""
**What this means:** Out of {total_loaded + total_rejected} total records processed, 
{total_loaded} were successfully loaded into the database and {total_rejected} were rejected due to data quality issues.
""")

st.markdown("---")

# Data Distribution
st.subheader("Data Distribution")
st.markdown("**How the data is distributed across different categories**")

# Patient Demographics
st.markdown("### Patient Demographics")
col1, col2 = st.columns(2)

with col1:
    if not patients.empty and 'sex' in patients.columns:
        st.markdown("**Patient Gender Distribution**")
        st.caption("Breakdown of patients by gender")
        
        gender_counts = patients['sex'].value_counts()
        
        # Map sex codes to labels
        gender_labels = {'M': 'Male', 'F': 'Female', 'U': 'Unknown'}
        gender_data = pd.DataFrame({
            'Gender': [gender_labels.get(k, k) for k in gender_counts.index],
            'Count': gender_counts.values
        })
        
        fig = px.pie(
            gender_data,
            values='Count',
            names='Gender',
            hole=0.4,
            color_discrete_sequence=['#636EFA', '#EF553B', '#00CC96']
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
        
        male_count = gender_counts.get('M', 0)
        female_count = gender_counts.get('F', 0)
        unknown_count = gender_counts.get('U', 0)
        st.caption(f"Male: {male_count} | Female: {female_count} | Unknown: {unknown_count}")
    else:
        st.info("Gender data not available")

with col2:
    if not patients.empty and 'dob' in patients.columns:
        st.markdown("**Patient Age Distribution**")
        st.caption("Age groups of patients in the system")
        patients_temp = patients.copy()
        patients_temp['dob'] = pd.to_datetime(patients_temp['dob'], errors='coerce')
        patients_temp['age'] = ((pd.Timestamp.now() - patients_temp['dob']).dt.days / 365.25)
        patients_temp = patients_temp.dropna(subset=['age'])
        patients_temp = patients_temp[patients_temp['age'] >= 0]
        patients_temp['age'] = patients_temp['age'].astype(int)
        
        if not patients_temp.empty:
            bins = [0, 18, 35, 50, 65, 100]
            labels = ['0-17', '18-34', '35-49', '50-64', '65+']
            patients_temp['age_group'] = pd.cut(patients_temp['age'], bins=bins, labels=labels, right=False)
            age_counts = patients_temp['age_group'].value_counts().sort_index()
            fig = px.bar(
                x=age_counts.index.astype(str),
                y=age_counts.values,
                labels={'x': 'Age Group', 'y': 'Number of Patients'},
                text=age_counts.values,
                color_discrete_sequence=['#636EFA']
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig, use_container_width=True)
            avg_age = patients_temp['age'].mean()
            st.caption(f"Average age: {avg_age:.1f} years | Total: {len(patients_temp)} patients")
        else:
            st.info("No valid age data available")
    else:
        st.info("Age data not available")

st.markdown("---")
st.markdown("### Encounter Distribution")
col1, col2 = st.columns(2)

with col1:
    if not encounters.empty and 'encounter_type' in encounters.columns:
        st.markdown("**Encounter Types Distribution**")
        st.caption("Shows the breakdown of visit types (Outpatient, Emergency, Inpatient)")
        
        type_counts = encounters['encounter_type'].value_counts()
        fig = px.bar(
            x=type_counts.index,
            y=type_counts.values,
            labels={'x': 'Visit Type', 'y': 'Number of Encounters'},
            text=type_counts.values,
            color_discrete_sequence=['#00CC96']
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(showlegend=False, height=300)
        st.plotly_chart(fig, use_container_width=True)
        
        st.caption(f"Total encounters: {type_counts.sum()}")
    else:
        st.info("Encounter type data not available")

with col2:
    if not encounters.empty and 'encounter_status' in encounters.columns:
        st.markdown("**Encounter Status Distribution**")
        st.caption("Shows how many visits are closed vs still open")
        
        status_counts = encounters['encounter_status'].value_counts()
        fig = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            hole=0.4,
            color_discrete_sequence=['#AB63FA', '#FFA15A']
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
        
        closed_count = status_counts.get('CLOSED', 0)
        open_count = status_counts.get('OPEN', 0)
        closed_pct = (closed_count / status_counts.sum() * 100) if status_counts.sum() > 0 else 0
        st.caption(f"Closed: {closed_count} ({closed_pct:.1f}%) | Open: {open_count}")
    else:
        st.info("Encounter status data not available")

st.markdown("---")

# Data Quality
st.subheader("Data Quality Report")
st.markdown("**Detailed information about data quality issues and rejections**")

tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Patients", "Encounters", "Diagnoses"])

with tab1:
    st.markdown("### Quality Metrics Summary")
    st.markdown("""
    This shows how many records passed validation vs how many were rejected.
    Quality Score indicates the percentage of clean, valid data.
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Records Loaded Successfully", total_loaded)
        st.markdown("These records passed all validation checks and are in the database")
        
        st.metric("Records Rejected", total_rejected)
        st.markdown("These records had data quality issues and were logged but not loaded")
    
    with col2:
        st.metric("Success Rate", f"{quality_pct:.1f}%")
        st.markdown("Percentage of records that passed all quality checks")
        
        st.metric("Total Processed", total_loaded + total_rejected)
        st.markdown("Total number of records in source files")
    
    if total_rejected > 0:
        st.markdown("---")
        st.markdown("### Issues by Data Type")
        st.markdown("This chart shows which data types had the most quality issues")
        
        issue_breakdown = {
            'Patients': len(patients_log),
            'Encounters': len(encounters_log),
            'Diagnoses': len(diagnoses_log)
        }
        
        fig = px.bar(
            x=list(issue_breakdown.keys()),
            y=list(issue_breakdown.values()),
            labels={'x': 'Data Type', 'y': 'Number of Issues'},
            text=list(issue_breakdown.values()),
            color_discrete_sequence=['#EF553B']
        )
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.success("Great! No data quality issues found. All records are clean.")

with tab2:
    st.markdown("### Patient Data Quality")
    
    if not patients_log.empty:
        st.warning(f"Found {len(patients_log)} patient records with issues")
        
        st.markdown("""
        **What happened:**
        - Some patients appeared multiple times in the source data (duplicates)
        - The ETL kept the most complete record and removed the rest
        
        **Why this matters:**
        - Duplicate patients can cause incorrect analysis
        - Each patient should appear only once in the database
        
        **Details of rejected records:**
        """)
        
        st.dataframe(patients_log, use_container_width=True, height=300)
    else:
        st.success("No patient data issues found. All patient records are unique and valid.")

with tab3:
    st.markdown("### Encounter Data Quality")
    
    if not encounters_log.empty:
        st.warning(f"Found {len(encounters_log)} encounter records with issues")
        
        st.markdown("**Common issues found:**")
        
        if 'qa_flags' in encounters_log.columns:
            fk_issues = encounters_log['qa_flags'].str.contains('FK_VIOLATION', na=False).sum()
            date_issues = encounters_log['qa_flags'].str.contains('DISCHARGE_BEFORE_ADMIT', na=False).sum()
            dup_issues = encounters_log['qa_flags'].str.contains('DEDUP', na=False).sum()
            
            if fk_issues:
                st.markdown(f"""
                **{fk_issues} Foreign Key Violations**
                - These encounters reference patient IDs that don't exist
                - Example: Encounter for patient P-0999, but patient P-0999 is not in the patients table
                - Action: These encounters were rejected to maintain data integrity
                """)
            
            if date_issues:
                st.markdown(f"""
                **{date_issues} Invalid Date Logic**
                - Discharge date is before admit date (impossible scenario)
                - Example: Patient admitted on Jan 5 but discharged on Jan 4
                - Action: These encounters were rejected as logically invalid
                """)
            
            if dup_issues:
                st.markdown(f"""
                **{dup_issues} Duplicate Encounters**
                - Same encounter appeared multiple times in source data
                - Action: Kept the most complete version, removed duplicates
                """)
        
        st.markdown("**Details of all rejected encounters:**")
        st.dataframe(encounters_log, use_container_width=True, height=300)
    else:
        st.success("No encounter data issues found. All encounters are valid.")

with tab4:
    st.markdown("### Diagnosis Data Quality")
    
    if not diagnoses_log.empty:
        st.warning(f"Found {len(diagnoses_log)} diagnosis records with issues")
        
        st.markdown("**Common issues found:**")
        
        if 'qa_flags' in diagnoses_log.columns:
            fk_issues = diagnoses_log['qa_flags'].str.contains('FK_VIOLATION', na=False).sum()
            code_issues = diagnoses_log['qa_flags'].str.contains('INVALID_CODE', na=False).sum()
            
            if fk_issues:
                st.markdown(f"""
                **{fk_issues} Foreign Key Violations**
                - These diagnoses reference encounter IDs that don't exist
                - Example: Diagnosis for encounter ENC-999999, but that encounter is not in the encounters table
                - Action: These diagnoses were rejected to maintain data integrity
                """)
            
            if code_issues:
                st.markdown(f"""
                **{code_issues} Invalid Diagnosis Codes**
                - Diagnosis codes don't follow ICD-10 format
                - Example: Code "ABC" instead of proper format like "A00.1"
                - Action: These diagnoses were rejected as invalid
                """)
        
        st.markdown("**Details of all rejected diagnoses:**")
        st.dataframe(diagnoses_log, use_container_width=True, height=300)
    else:
        st.success("No diagnosis data issues found. All diagnoses are valid.")

st.markdown("---")

# Data Explorer
st.subheader("Data Explorer")
st.markdown("**Browse the actual data that was successfully loaded into the database**")

table_option = st.selectbox("Select table to view", ["Patients", "Encounters", "Diagnoses"])

if table_option == "Patients":
    if not patients.empty:
        st.markdown(f"**Viewing all {len(patients)} patient records**")
        st.caption("These are the patients currently in the database after cleaning and validation")
        st.dataframe(patients, use_container_width=True, height=400)
    else:
        st.info("No patient data available")

elif table_option == "Encounters":
    if not encounters.empty:
        st.markdown("**Filter encounters by type or status:**")
        
        col1, col2 = st.columns(2)
        
        filtered_encounters = encounters.copy()
        
        with col1:
            if 'encounter_type' in encounters.columns:
                types = ['All'] + list(encounters['encounter_type'].unique())
                selected_type = st.selectbox("Filter by visit type", types)
                if selected_type != 'All':
                    filtered_encounters = filtered_encounters[filtered_encounters['encounter_type'] == selected_type]
        
        with col2:
            if 'encounter_status' in encounters.columns:
                statuses = ['All'] + list(encounters['encounter_status'].unique())
                selected_status = st.selectbox("Filter by status", statuses)
                if selected_status != 'All':
                    filtered_encounters = filtered_encounters[filtered_encounters['encounter_status'] == selected_status]
        
        st.markdown(f"**Showing {len(filtered_encounters)} encounter records**")
        st.caption("These are the encounters currently in the database after cleaning and validation")
        st.dataframe(filtered_encounters, use_container_width=True, height=400)
    else:
        st.info("No encounter data available")

else:
    if not diagnoses.empty:
        st.markdown(f"**Viewing all {len(diagnoses)} diagnosis records**")
        st.caption("These are the diagnoses currently in the database after cleaning and validation")
        st.dataframe(diagnoses, use_container_width=True, height=400)
    else:
        st.info("No diagnosis data available")

st.markdown("---")
st.caption("Healthcare ETL Pipeline - Data Quality Dashboard")
st.caption("Last updated: Re-run ETL to refresh data")
st.caption("Developed by **Eman Khadim**")