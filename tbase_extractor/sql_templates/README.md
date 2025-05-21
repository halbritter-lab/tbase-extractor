# SQL Templates Documentation

This folder contains SQL query templates for extracting data from the TBase database. These templates are designed to be used with parameterized queries to ensure security against SQL injection attacks.

## Database Information Tools

- **list_tables.sql**: Lists all tables in the database with their columns and data types.
- **get_table_columns.sql**: Retrieves column names and data types for a specific table.
  - Parameters: TABLE_NAME, TABLE_SCHEMA

## Patient Queries (Single Table)

These templates extract data from the "Patient" table only:

- **get_all_patients.sql**: Retrieves all patients from the database. Use with caution on large databases.
- **get_patient_by_id.sql**: Retrieves a patient by their PatientID.
  - Parameters: PatientID
- **get_patient_by_name_dob.sql**: Retrieves patients matching specific first name, last name, and date of birth.
  - Parameters: First Name, Last Name, Date of Birth
- **get_patients_by_lastname_like.sql**: Retrieves patients whose last name matches a LIKE pattern.
  - Parameters: Last Name Pattern (e.g., 'Smith%')
- **get_patients_by_dob_year_range.sql**: Retrieves patients born within a specific year range.
  - Parameters: Start Year, End Year

## Patient Diagnoses Queries (Joined Tables)

These templates extract data from both "Patient" and "Diagnose" tables using a LEFT JOIN:

- **get_patient_diagnoses_by_id.sql**: Retrieves a patient and their diagnoses by PatientID.
  - Parameters: PatientID
- **get_patients_diagnoses_by_lastname_like.sql**: Retrieves patients and their diagnoses where the last name matches a LIKE pattern.
  - Parameters: Last Name Pattern (e.g., 'Smith%')
- **get_patients_diagnoses_by_dob_year_range.sql**: Retrieves patients and their diagnoses for patients born within a specific year range.
  - Parameters: Start Year, End Year

## Known Issues

- **get_patient_by_name_dob.sql**: Currently cannot be used to extract "ICD10" and "Bezeichnung" columns from the "Diagnose" table.
- The diagnosis-related templates are prefixed with "_diagnoses_" but will also function if named without this prefix.

## Clinically Relevant Information

The following columns contain important clinical information:

### Patient Table
- **Grunderkrankung**: Primary disease/condition
- **ET_Grunderkrankung**: ET primary disease/condition 
- **Dauernotiz**: Permanent notes
- **Dauernotiz_Diagnose**: Permanent diagnosis notes

### Diagnose Table
- **ICD10**: ICD-10 diagnosis code
- **Bezeichnung**: Diagnosis description/name

## Usage Notes

- All templates use the "dbo" schema for table references.
- Parameters are represented by "?" placeholder in the SQL queries.
- Always verify that table and column names match your actual database structure before use.
- The LEFT JOIN in the diagnoses queries ensures patients without diagnoses are still included in results.