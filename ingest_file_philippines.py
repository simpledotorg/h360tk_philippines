import sys
import json
import hashlib
import pandas as pd
from datetime import datetime, timedelta
import re
import os
import psycopg2
from psycopg2 import errorcodes

HEADER_ROW = 1
COL_INDIVIDUAL_ID = 'Patient ID'
COL_FIRST_NAME = 'First Name'
COL_MIDDLE_NAME = 'Middle Name'
COL_LAST_NAME = 'Last Name'
COL_SEX = 'Gender'
COL_MOBILE = 'Phone Number'
COL_DATE_OF_BIRTH = 'Date of Birth'
COL_AGE = 'Age'

# Facility hierarchy columns
COL_REGION = 'Region'
COL_DISTRICT = 'District'
COL_PHC = 'Facility'
COL_SHC = 'Sub Facility'

# Registration date column
COL_REGISTRATION_DATE = 'Registration Date'
COL_VISIT_TIME = 'Visit Time'

# HTN columns
COL_SYSTOLIC = 'Systolic'
COL_DIASTOLIC = 'Diastolic'

# DM columns
COL_BS_TYPE = 'Blood Sugar Type'
COL_BS_VALUE = 'Blood Sugar Value'

# Diagnosis columns
COL_DIAGNOSIS_1 = 'Diagnosis 1'
COL_DIAGNOSIS_2 = 'Diagnosis 2'


CSV_DATE_FORMATS = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%y %H:%M:%S"]
DATE_FORMAT_OUT = "%Y-%m-%d"

# --- DATABASE CONNECTION DETAILS ---
DB_CONNECTION_PARAMS = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'metrics_db'),
    'user': os.getenv('POSTGRES_USER', 'grafana_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'your_db_password'),
}
SP_REGION_VALUE = 'Demo Region'

# Fields:
#   level        – integer depth (1 = top)
#                  Example: 1, 2, 3, ... 6
#   column       – Excel column name(s) to read (first match wins)
#                  Example: ['wilayah'], ['district'], ['small_village']
#   display_name – label for readability only. Levels 1–5 have fixed names in
#                  Grafana (Region, District, Facility, Sub-Facility, Village); only levels 6+
#                  can be customized via this field but it is not display in grafana.
#   var_name     – Levels 1–5 use fixed names (region, district, facility, sub_facility, village);
#                  only levels 6+ need this (e.g. level_6, level_7).
#   default      – fallback value when column is empty (None = skip level)
HIERARCHY_LEVELS = [
    {'level': 1, 'column': [COL_REGION], 'display_name': 'Region', 'var_name': 'region', 'default': SP_REGION_VALUE},
    {'level': 2, 'column': [COL_DISTRICT], 'display_name': 'District', 'var_name': 'district', 'default': None},
    {'level': 3, 'column': [COL_PHC], 'display_name': 'Facility', 'var_name': 'facility', 'default': 'UNKNOWN'},
    {'level': 4, 'column': [COL_SHC], 'display_name': 'Sub-Facility', 'var_name': 'sub_facility', 'default': None},
]

# --- ALLOWED BLOOD SUGAR TYPES ---
# Only these types are accepted during ingestion. Any other value
# causes the blood sugar record to be discarded.
ALLOWED_SUGAR_TYPES = {'RBS', 'FBS', 'PPBS', 'HBA1C'}
DEFAULT_SUGAR_TYPE = 'RBS'

# --- ALLOWED DIAGNOSIS CODES ---
# Only these codes are accepted. Any other value is silently ignored.
ALLOWED_DIAGNOSIS_CODES = {'I10', 'E11'}

# --- HELPER FUNCTIONS ---

def uuid_to_int_hash(uuid_str):
    if pd.isna(uuid_str) or not uuid_str:
        return None
    digest = hashlib.sha256(str(uuid_str).strip().encode('utf-8')).hexdigest()
    return int(digest[:15], 16) % (2**53)

def parse_date(date_str):
    if pd.isna(date_str) or date_str is None or str(date_str).strip() == '':
        return None

    date_str = str(date_str).strip()
    for fmt in CSV_DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except (ValueError, TypeError):
            continue
    return None

# --- SQL HELPER FUNCTIONS ---

def safe_str(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)

def calculate_dob_from_age(age, reference_date=None):
    if age is None or (isinstance(age, float) and pd.isna(age)):
        return None

    try:
        age = int(age)
    except:
        return None

    if reference_date is None:
        reference_date = datetime.today()

    try:
        return reference_date.replace(year=reference_date.year - age)
    except ValueError:
        # Handle leap year edge case (Feb 29)
        return reference_date - timedelta(days=age * 365)

def build_patient_name(row):
    first = safe_str(row.get(COL_FIRST_NAME))
    middle = safe_str(row.get(COL_MIDDLE_NAME))
    last = safe_str(row.get(COL_LAST_NAME))

    name_parts = [part.strip() for part in [first, middle, last] if part and part.strip()]
    if not name_parts:
        return None

    return " ".join(name_parts)

def build_hierarchy_from_row(row):
    """Build (name, level) tuples for upsert_org_unit_chain from HIERARCHY_LEVELS."""
    hierarchy = []
    for hlvl in HIERARCHY_LEVELS:
        value = None
        for col in hlvl['column']:
            value = safe_str(row.get(col))
            if value:
                break
        if not value:
            value = hlvl.get('default')
        if value:
            hierarchy.append((value, hlvl['level']))
    return hierarchy

def sync_hierarchy_config(cur):
    """Upsert hierarchy_config from HIERARCHY_LEVELS when the table exists.

    Older Heart360TK PostgreSQL images may not define hierarchy_config; in that
    case we skip sync and ingestion still uses upsert_org_unit_chain only.
    """
    try:
        for hlvl in HIERARCHY_LEVELS:
            cur.execute(
                """
                    INSERT INTO hierarchy_config (level, display_name, var_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (level) DO UPDATE
                        SET display_name = EXCLUDED.display_name,
                            var_name     = EXCLUDED.var_name
                """,
                (hlvl['level'], hlvl['display_name'], hlvl['var_name']),
            )
    except psycopg2.Error as e:
        if e.pgcode != errorcodes.UNDEFINED_TABLE:
            raise
        print(
            'Warning: hierarchy_config not in this database; skipped metadata sync. '
            'Upgrade the DB image or add the table if Grafana drill-down needs it.',
            file=sys.stderr,
        )

def to_sql_literal(value, target_type=None):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        if target_type == 'bigint':
            return 'NULL::BIGINT'
        elif target_type == 'DATE':
            return 'NULL::DATE'
        elif target_type == 'TIMESTAMP':
            return 'NULL::TIMESTAMP'
        elif target_type == 'NUMERIC':
            return 'NULL::NUMERIC'
        else:
            return 'NULL::VARCHAR'

    if target_type == 'bigint':
        val_str = str(value).strip()
        if val_str.endswith('.0'):
            val_str = val_str[:-2]
        if not val_str.isdigit():
            return 'NULL::BIGINT'
        return f"CAST('{val_str}' AS bigint)"

    if target_type == 'DATE' and isinstance(value, (datetime, pd.Timestamp)):
        return f"'{value.strftime('%Y-%m-%d')}'::DATE"

    if target_type == 'TIMESTAMP' and isinstance(value, (datetime, pd.Timestamp)):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp"

    if isinstance(value, str):
        return f"'{value.replace(chr(39), chr(39)+chr(39))}'::VARCHAR"

    if isinstance(value, (int, float)):
        return str(value)

    return f"'{str(value).replace(chr(39), chr(39)+chr(39))}'::VARCHAR"

# --- DATABASE EXECUTION FUNCTIONS (matching reference pattern) ---

def execute_upsert_org_unit_chain(cur, hierarchy):
    """Upsert org_unit hierarchy chain and return leaf org_unit_id.
    hierarchy: list of (name, level) tuples from top to bottom.
    Skips entries with None names.
    """
    names = [h[0] for h in hierarchy if h[0] is not None]
    levels = [h[1] for h in hierarchy if h[0] is not None]
    if not names:
        return None
    names_literal = "ARRAY[" + ",".join(to_sql_literal(n) for n in names) + "]"
    levels_literal = "ARRAY[" + ",".join(str(l) for l in levels) + "]"
    sql = f"SELECT upsert_org_unit_chain({names_literal}::VARCHAR[], {levels_literal}::INTEGER[]);"
    cur.execute(sql)
    return cur.fetchone()[0]

def execute_upsert_patient(cur, patient_id_sql, patient_name, gender, phone_number, registration_date, birth_date, org_unit_id):
    """Insert new patient or update registration_date if earlier."""
    sql = f"""
INSERT INTO patients (patient_id, patient_name, gender, phone_number, patient_status, registration_date, birth_date, org_unit_id)
VALUES (
    {patient_id_sql},
    {to_sql_literal(patient_name)},
    {to_sql_literal(gender)},
    {to_sql_literal(phone_number)},
    'ALIVE'::VARCHAR,
    {to_sql_literal(registration_date, target_type='TIMESTAMP')},
    {to_sql_literal(birth_date, target_type='DATE')},
    {org_unit_id}
)
ON CONFLICT (patient_id) DO UPDATE SET
    registration_date = LEAST(patients.registration_date, EXCLUDED.registration_date);
"""
    cur.execute(sql)

def execute_insert_encounter(cur, patient_id_sql, encounter_datetime, org_unit_id):
    """Create encounter (or get existing). Returns encounter_id."""
    sql = f"""
INSERT INTO encounters (patient_id, encounter_date, org_unit_id)
VALUES ({patient_id_sql}, {to_sql_literal(encounter_datetime, target_type='TIMESTAMP')}, {org_unit_id})
ON CONFLICT (patient_id, encounter_date)
DO UPDATE SET org_unit_id = EXCLUDED.org_unit_id
RETURNING id;
"""
    cur.execute(sql)
    return cur.fetchone()[0]

def execute_insert_bp(cur, encounter_id, systolic, diastolic):
    """Insert blood pressure for an encounter."""
    if systolic is None and diastolic is None:
        return
    sql = f"""
INSERT INTO blood_pressures (encounter_id, systolic_bp, diastolic_bp)
VALUES ({encounter_id}, {to_sql_literal(systolic, target_type='NUMERIC')}, {to_sql_literal(diastolic, target_type='NUMERIC')})
ON CONFLICT (encounter_id) DO UPDATE SET
    systolic_bp = EXCLUDED.systolic_bp, diastolic_bp = EXCLUDED.diastolic_bp;
"""
    cur.execute(sql)

def execute_insert_bs(cur, encounter_id, blood_sugar_type, blood_sugar_value):
    """Insert blood sugar for an encounter."""
    if blood_sugar_value is None:
        return
    sql = f"""
INSERT INTO blood_sugars (encounter_id, blood_sugar_type, blood_sugar_value)
VALUES ({encounter_id}, {to_sql_literal(safe_str(blood_sugar_type))}, {to_sql_literal(blood_sugar_value, target_type='NUMERIC')})
ON CONFLICT (encounter_id) DO UPDATE SET
    blood_sugar_type = EXCLUDED.blood_sugar_type, blood_sugar_value = EXCLUDED.blood_sugar_value;
"""
    cur.execute(sql)

def execute_insert_diagnosis(cur, patient_id_sql, diagnosis_code):
    """Insert a diagnosis for a patient. Only allows codes in ALLOWED_DIAGNOSIS_CODES."""
    if diagnosis_code not in ALLOWED_DIAGNOSIS_CODES:
        return

    sql = f"""
    INSERT INTO patient_diagnoses (patient_id, diagnosis_code)
    VALUES (
        {patient_id_sql},
        {to_sql_literal(diagnosis_code)}
    )
    ON CONFLICT (patient_id, diagnosis_code)
    DO NOTHING;
    """

    cur.execute(sql)

# --- MAIN INGESTION AND EXECUTION FUNCTION ---

def ingest_and_execute(file_path: str) -> None:
    """
    Reads an Excel file, extracts BP/BS from fields, and inserts
    into the database using direct SQL (matching reference hierarchy pattern).

    Facility hierarchy: Region → District → Facility → Sub-Facility (see HIERARCHY_LEVELS).

    Diagnosis tags are read from 'Diagnosis 1' and 'Diagnosis 2' columns.
    No fallback logic is applied — if diagnosis columns are empty, no
    diagnosis tag is created for that patient.
    """

    DTYPE_MAPPING = {COL_INDIVIDUAL_ID: str, COL_MOBILE: str}

    stats = {
        'total_rows': 0,
        'unique_patients': set(),
        'invalid_visit_date': 0,
        'invalid_registration_date': 0,
        'processed_records': 0
    }

    try:
        header_index = HEADER_ROW - 1
        if file_path.lower().endswith('.csv'):
            df_data = pd.read_csv(file_path, dtype=DTYPE_MAPPING, skiprows=header_index)
        else:
            df_data = pd.read_excel(
                file_path,
                sheet_name=0,
                header=header_index,
                dtype=DTYPE_MAPPING,
                engine='openpyxl'
            )
    except Exception as e:
        print(f"Error loading file: {e}", file=sys.stderr)
        return

    stats['total_rows'] = len(df_data)

    print(f"Columns found: {list(df_data.columns)}", file=sys.stderr)
    print(f"Total rows: {len(df_data)}", file=sys.stderr)

    if df_data.empty:
        print("Error: No data rows found in Excel", file=sys.stderr)
        return

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(**DB_CONNECTION_PARAMS)
        conn.autocommit = True
        cur = conn.cursor()
        sync_hierarchy_config(cur)

        for idx, row in df_data.iterrows():
            if pd.isna(row.get(COL_INDIVIDUAL_ID)) or str(row.get(COL_INDIVIDUAL_ID)).strip() == '':
                continue

            visit_date = parse_date(row.get(COL_VISIT_TIME))
            registration_date = parse_date(row.get(COL_REGISTRATION_DATE))

            if not visit_date:
                if registration_date:
                    visit_date = registration_date
                else:
                    stats['invalid_visit_date'] += 1
                    print(f"Row {idx + 2}: Skipping - registration date and visit time not found or invalid", file=sys.stderr)
                    continue

            # If registration date not found, try to use last visit time
            if not registration_date:
                if visit_date:
                    registration_date = visit_date
                else:
                    stats['invalid_registration_date'] += 1
                    print(f"Row {idx + 2}: Skipping - registration date and visit time not found or invalid", file=sys.stderr)
                    continue

            systolic = row.get(COL_SYSTOLIC)
            diastolic = row.get(COL_DIASTOLIC)

            raw_sugar_type = row.get(COL_BS_TYPE)
            sugar_type = None
            sugar_value = row.get(COL_BS_VALUE)

            if pd.isna(sugar_value) or sugar_value is None:
                sugar_value = None
                sugar_type = None
            else:
                if not raw_sugar_type or pd.isna(raw_sugar_type):
                    sugar_type = DEFAULT_SUGAR_TYPE
                else:
                    sugar_type = str(raw_sugar_type).strip().upper()
                    if sugar_type not in ALLOWED_SUGAR_TYPES:
                        # Invalid BS type: discard entire BS record
                        sugar_type = None
                        sugar_value = None
                        print(
                            f"Row {idx + 2}: Invalid blood sugar type '{raw_sugar_type}' — BS record discarded",
                            file=sys.stderr
                        )

            patient_id = uuid_to_int_hash(row.get(COL_INDIVIDUAL_ID))
            # Build patient fields

            patient_name = build_patient_name(row)

            gender = safe_str(row.get(COL_SEX)) if not pd.isna(row.get(COL_SEX)) else None

            phone_raw = row.get(COL_MOBILE)
            if pd.isna(phone_raw):
                phone_number = None
            else:
                phone_str = str(phone_raw).strip()
                if phone_str.endswith('.0'):
                    phone_str = phone_str[:-2]
                if phone_str.lower() == 'nan' or phone_str == '':
                    phone_number = None
                else:
                    phone_number = phone_str

            birth_date = parse_date(row.get(COL_DATE_OF_BIRTH))

            if not birth_date:
                age_value = row.get(COL_AGE)
                birth_date = calculate_dob_from_age(age_value, reference_date=registration_date)

            phc = safe_str(row.get(COL_PHC)) or 'UNKNOWN'
            hierarchy = build_hierarchy_from_row(row)

            # Determine if we have BP and/or BS data
            has_bp = not pd.isna(systolic) if systolic is not None else False
            has_bs = sugar_value is not None

            # Log the record
            log_record = {
                'patient_id': patient_id,
                'patient_name': patient_name,
                'facility': phc,
                'registration_date': registration_date.strftime(DATE_FORMAT_OUT) if registration_date else None,
                'encounter_datetime': visit_date.strftime(DATE_FORMAT_OUT) if visit_date else None,
                'systolic_bp': systolic if has_bp else None,
                'diastolic_bp': diastolic if has_bp else None,
                'blood_sugar_type': sugar_type if has_bs else None,
                'blood_sugar_value': sugar_value if has_bs else None
            }
            print(json.dumps(log_record, ensure_ascii=False, default=str))

            # --- Per-Row Insertion ---
            try:
                patient_id_sql = to_sql_literal(patient_id, target_type='bigint')

                if patient_id_sql == 'NULL::BIGINT':
                    print(f"Row {idx + 2}: Skipping - NULL patient_id", file=sys.stderr)
                    continue

                # 0. Upsert org_unit hierarchy chain (returns leaf org_unit_id)
                org_unit_id = execute_upsert_org_unit_chain(cur, hierarchy)

                # 1. Upsert patient
                execute_upsert_patient(cur, patient_id_sql, patient_name, gender, phone_number, registration_date, birth_date, org_unit_id)

                # 2. Read diagnosis columns and insert diagnosis tags
                #    NO fallback logic — only explicit diagnosis values are used.
                diagnosis_1 = safe_str(row.get(COL_DIAGNOSIS_1))
                diagnosis_2 = safe_str(row.get(COL_DIAGNOSIS_2))

                diagnoses = set()

                if diagnosis_1:
                    diagnoses.add(diagnosis_1.strip().upper())

                if diagnosis_2:
                    diagnoses.add(diagnosis_2.strip().upper())

                for diagnosis_code in diagnoses:
                    execute_insert_diagnosis(
                        cur,
                        patient_id_sql,
                        diagnosis_code
                    )

                # 3. Create encounter and insert clinical data
                enc_id = execute_insert_encounter(cur, patient_id_sql, visit_date, org_unit_id)

                if has_bp:
                    execute_insert_bp(cur, enc_id, systolic, diastolic)

                if has_bs:
                    execute_insert_bs(cur, enc_id, sugar_type, sugar_value)

                stats['processed_records'] += 1

            except psycopg2.Error as e:
                print(f"\n--- RECORD FAILURE ---", file=sys.stderr)
                print(f"Error processing row {idx + 2}. Skipping. Details: {e}", file=sys.stderr)

        print(f"\n--- EXECUTION SUMMARY ---", file=sys.stderr)
        print(f"Total rows in Excel: {stats['total_rows']}", file=sys.stderr)
        print(f"Invalid last visit date excluded: {stats['invalid_visit_date']}", file=sys.stderr)
        print(f"Invalid registration date excluded: {stats['invalid_registration_date']}", file=sys.stderr)
        print(f"Successfully processed records: {stats['processed_records']}", file=sys.stderr)

    except psycopg2.Error as e:
        print(f"\n--- CONNECTION ERROR ---", file=sys.stderr)
        print(f"PostgreSQL Connection Error: {e}", file=sys.stderr)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python ingest_file_h360tk.py <xlsx_file_path>", file=sys.stderr)
        sys.exit(1)
    else:
        ingest_and_execute(sys.argv[1])