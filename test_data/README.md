## Sample Anonymised Patient Data - Column Definitions

| Column Name       | Description |  Notes
|-------------------|-------------|--------
| First Name      | Patient's first name as free text (e.g. "Ishita"). | Optional. Can be omitted if there are data-sharing or privacy restrictions. The toolkit functions normally without it. |
| Middle Name      | Patient's middle name as free text (e.g. "MD"). | Optional. Can be omitted if there are data-sharing or privacy restrictions. The toolkit functions normally without it. |
| Last Name      | Patient's last name as free text (e.g. "Tanya"). | Optional. Can be omitted if there are data-sharing or privacy restrictions. The toolkit functions normally without it. |
| Patient ID        | A unique identifier for each patient — any alphanumeric string (e.g. "PH-00123"). Each patient must have exactly one ID that stays the same across visits. | REQUIRED FIELD. Used to match visits to the correct patient and prevent duplicate records. |
| Phone Number      | Patient's phone number as a string (e.g. "09171234567"). No specific format enforced. | Optional |
| Date of Birth     | Patient's date of birth in DD/MM/YYYY or YYYY-MM-DD format. | Optional. If missing the toolkit will use Age as a fallback and calculate it from age.|
| Age               | Patient's age as a number (e.g. 36) | Optional. Used to calculate date of birth if file doesn't have a Date Of Birth value | 
| Gender            | Free text (e.g. "Male", "Female"). No specific coding required. | Optional |
| Registration Date | Date the patient was first registered. Accepts most date formats: DD/MM/YYYY, YYYY-MM-DD, or with time included. | If missing, the toolkit will use the Last Visit Time as a fallback. If both are missing, the row is skipped. |
| Visit Time        | Date (and optionally time) of the most recent clinical encounter. Accepts: DD/MM/YYYY, YYYY-MM-DD, or with time (e.g. "2024-03-15 10:30:00"). | REQUIRED. Used as the encounter date. Also serves as a fallback for Registration Date if that field is missing. Rows without a valid date here are skipped. |
| Systolic          | Systolic blood pressure reading as a number (e.g. "128"). | Required, if missing BP record is skipped |
| Diastolic         | Diastolic blood pressure reading as a number (e.g. "82"). | Required, if missing BS record is skipped |
| Blood Sugar Type  | Type of blood sugar test (RBS / FBS / HBA1C / PPBS). | Defaults to "RBS" if not provided. Leave blank when no blood sugar value is recorded. |
| Blood Sugar Value | Blood sugar reading as a number (e.g. "110"). | Optional |
| Diagnosis 1       | Primary diagnosis ICD code for hypertension (e.g. "I10"). | Tags the patient for the hypertension dashboard. Column name and code are configurable in ingestion (`COL_DIAGNOSIS_1`, `ALLOWED_DIAGNOSIS_CODES`). |
| Diagnosis 2       | Secondary diagnosis ICD code for diabetes (e.g. "E11"). | Tags the patient for the diabetes dashboard. Leave blank if the patient has no diabetes diagnosis. Column name and code are configurable in ingestion (`COL_DIAGNOSIS_2`, `ALLOWED_DIAGNOSIS_CODES`). |
| Region            | Name of the administrative region (Level 1 geography). Free text string. | |
| District          | Name of the district (Level 2 geography). Free text string. | |
| Facility          | Name of the health facility (e.g. primary care centre). Free text string. | |
| Sub Facility      | Name of the sub-facility or health post (Level 4). Free text string. | Optional. Leave blank if not applicable. |