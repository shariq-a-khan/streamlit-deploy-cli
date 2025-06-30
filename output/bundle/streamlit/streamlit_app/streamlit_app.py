import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import json
import tempfile
from io import StringIO, BytesIO
from datetime import datetime
import os
from typing import Dict, List, Any, Optional,Tuple

def validate_schema_match(df: pd.DataFrame, schema: Dict) -> Tuple[bool, List[str]]:
    """
    Validate if DataFrame matches schema definition.
    Returns (is_valid, list_of_issues)
    """
    issues = []

    # Get schema columns
    schema_columns = {
        row["COLUMN_NAME"]: {
            "type": row["DATA_TYPE"],
            "nullable": row["IS_NULLABLE"].strip().upper() == "YES"
        }
        for _, row in schema.iterrows()
    }

    # Check if all required columns are present
    for col_name, col_info in schema_columns.items():
        if col_name not in df.columns:
            issues.append(f"Missing required column: {col_name}")
            continue

        # Check data type
        col_data = df[col_name]
        try:
            if col_info['type'] == 'INTEGER':
                # Try converting to numeric and check if all are integers
                numeric_data = pd.to_numeric(col_data.dropna())
                if not all(numeric_data.astype(int) == numeric_data):
                    issues.append(f"Column '{col_name}' contains non-integer values")

            elif col_info['type'] == 'FLOAT':
                # Try converting to numeric
                pd.to_numeric(col_data.dropna())

            elif col_info['type'] == 'DATETIME':
                # Try converting to datetime
                pd.to_datetime(col_data.dropna())

        except Exception:
            issues.append(f"Column '{col_name}' contains invalid {col_info['type']} values")

        # Check for null values
        if not col_info.get('nullable', False) and col_data.isnull().any():
            issues.append(f"Column '{col_name}' contains null values which are not allowed")

    # Check for extra columns in the dataframe
    extra_cols = set(df.columns) - set(schema_columns.keys())
    if extra_cols:
        issues.append(f"Extra columns found: {', '.join(extra_cols)}")

    return len(issues) == 0, issues


def upload_page():
    """File upload interface with schema validation."""
    
    # Get Snowflake session
    session = get_active_session()
    
    st.subheader("Upload File")

    available_schemas = ["PUBLIC"]

    # Department selection
    schema_name = st.sidebar.selectbox(
        "Select Schema",
        options=available_schemas,
        help="Select Schema"
    )

    if not schema_name:
        return

    # Query to get external table names
    query = f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'EXTERNAL TABLE'
          AND TABLE_SCHEMA = '{schema_name}'
    """
    
    # Run the query
    df = session.sql(query).to_pandas()
    
    # Convert to a Python list
    external_tables = df['TABLE_NAME'].tolist()

    # File type selection
    table_name = st.sidebar.selectbox(
        "Select Table",
        options=external_tables,
        help="Select the support table"
    )

    if not table_name:
        return

    # Display schema information
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema_name}'
      AND TABLE_NAME = '{table_name}'
      AND COLUMN_NAME != 'VALUE'
    """

    schema = session.sql(query).to_pandas()

    st.text("Expected File Schema:")
    st.dataframe(schema, use_container_width=True)

    # File upload
    uploaded_file = st.file_uploader(
        "Upload CSV File",
        type=['csv'],
        help="Select a CSV file that matches the schema above"
    )

    if uploaded_file:
        try:
            # Read the CSV file
            df = pd.read_csv(uploaded_file)
            

            # Validate schema
            is_valid, issues = validate_schema_match(df, schema)

            if not is_valid:
                st.error("❌ Schema validation failed:")
                for issue in issues:
                    st.warning(f"• {issue}")
                return

            # Show preview if validation passed
            st.success("✅ File schema validation passed!")
            st.write("Data Preview:")
            st.dataframe(df.head(5), use_container_width=True)

            # Upload button
            if st.button("Upload File"):
                try:
                    s3 = S3Handler()
                    file_path = f"{schema_name.lower()}/{table_name.lower()}/{table_name.lower()}.csv"

                    # Check if file exists and create backup if needed
                    if s3.file_exists(file_path):
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        backup_path = f"{schema_name.lower()}/{table_name.lower()}/archive/{table_name.lower()}_{timestamp}.csv"
                        s3.rename_file(file_path, backup_path)
                        st.info(f"📁 Existing file backed up with timestamp")

                    # Upload new file
                    uploaded_file.seek(0)
                    s3.upload_file(uploaded_file, file_path)
                    st.success("✨ File uploaded successfully!")

                    # Show file details
                    st.write("**Upload Details:**")
                    st.write(f"• Location: {file_path}")
                    st.write(f"• Rows: {len(df):,}")
                    st.write(f"• Columns: {len(df.columns):,}")

                except Exception as e:
                    st.error(f"Upload failed: {str(e)}")

                try:
                    session.sql(f"ALTER EXTERNAL TABLE {table_name.lower()} REFRESH").collect()
                    st.success("External table refreshed successfully!")
                except Exception as e:
                    st.error(f"Failed to refresh external table: {e}")

        except Exception as e:
            st.error(f"Error reading file: {str(e)}")


def get_column_config(df, schema):
    """Create appropriate column configuration based on schema and data types."""
    column_config = {}

    for col in df.columns:
        # Get schema info for the column
        col_schema = schema['columns'].get(col, {'type': 'STRING'})

        # Configure based on schema type
        if col_schema['type'] == 'INTEGER':
            column_config[col] = st.column_config.NumberColumn(
                col,
                help=f"Integer values only",
                min_value=None,
                max_value=None,
                step=1,
                format="%d"
            )
        elif col_schema['type'] == 'FLOAT':
            column_config[col] = st.column_config.NumberColumn(
                col,
                help=f"Decimal values allowed",
                min_value=None,
                max_value=None,
                format="%.2f"
            )
        elif col_schema['type'] == 'DATETIME':
            column_config[col] = st.column_config.DatetimeColumn(
                col,
                help=f"Date/time values",
                format="YYYY-MM-DD HH:mm:ss"
            )
        elif col_schema['type'] == 'EMAIL':
            column_config[col] = st.column_config.TextColumn(
                col,
                help=f"Email addresses",
                validate="^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
            )
        else:  # Default to text
            column_config[col] = st.column_config.TextColumn(
                col,
                help=f"Text values"
            )

    return column_config


def check_file_schema_compatibility(df: pd.DataFrame, schema: Dict) -> Tuple[bool, List[str]]:
    """
    Check if file's current schema matches with the selected file type schema.
    Returns (is_compatible, list_of_issues)
    """
    issues = []

    # Get schema columns
    schema_columns = {
        row["COLUMN_NAME"]: {
            "type": row["DATA_TYPE"],
            "nullable": row["IS_NULLABLE"].strip().upper() == "YES"
        }
        for _, row in schema.iterrows()
    }

    # Check if all schema columns are present
    for col_name, col_info in schema_columns.items():
        if col_name not in df.columns:
            issues.append(f"Missing column: {col_name}")
            continue

        # Basic type compatibility check
        col_data = df[col_name]
        try:
            if col_info['type'] == 'INTEGER':
                pd.to_numeric(col_data.dropna(), downcast='integer')
            elif col_info['type'] == 'FLOAT':
                pd.to_numeric(col_data.dropna(), downcast='float')
            elif col_info['type'] == 'DATETIME':
                pd.to_datetime(col_data.dropna())
        except:
            issues.append(f"Column '{col_name}' has incompatible data type")

    # Check for extra columns
    extra_cols = set(df.columns) - set(schema_columns.keys())
    if extra_cols:
        issues.append(f"Extra columns found: {', '.join(extra_cols)}")

    return len(issues) == 0, issues


def manage_files_page():
    """File management interface with schema compatibility check."""
    st.subheader("Manage Files")
    
    # Get Snowflake session
    session = get_active_session()

    s3 = S3Handler()

    available_schemas = ["PUBLIC"]

    # Department selection
    schema_name = st.sidebar.selectbox(
        "Select Schema",
        options=available_schemas,
        help="Select Schema",
        key = 'select_schema_manage_files'
    )

    if not schema_name:
        return

    # Query to get external table names
    query = f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'EXTERNAL TABLE'
          AND TABLE_SCHEMA = '{schema_name}'
    """
    
    # Run the query
    df = session.sql(query).to_pandas()
    
    # Convert to a Python list
    external_tables = df['TABLE_NAME'].tolist()

    # File type selection
    table_name = st.sidebar.selectbox(
        "Select Table",
        options=external_tables,
        help="Select the support table",
        key = 'select_table_manage_files'
    )

    if not table_name:
        return

    # Display schema information
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema_name}'
      AND TABLE_NAME = '{table_name}'
      AND COLUMN_NAME != 'VALUE'
    """

    schema = session.sql(query).to_pandas()

    st.text("Expected File Schema:")
    st.dataframe(schema, use_container_width=True)

    all_files = s3.list_files(f"{schema_name.lower()}/{table_name.lower()}")
    csv_files = [f for f in all_files
                 if f.lower().endswith('.csv') and
                 not f.startswith(f"{schema_name.lower()}/{table_name.lower()}/archive/")]

    if not csv_files:
        st.info("No files found for this file type")
        return

    # Check schema compatibility for each file
    compatible_files = []
    for file_path in csv_files:
        try:
            df = s3.get_file(file_path)
            is_compatible, _ = check_file_schema_compatibility(df, schema)
            if is_compatible:
                 compatible_files.append(file_path)
        except Exception as e:
            st.error(f"Error checking file {os.path.basename(file_path)}: {str(e)}")

    if not compatible_files:
        st.warning("No files found with matching schema")
        return

    # File selection (only compatible files)
    selected_file = st.selectbox(
        "Select File to Edit",
        options=compatible_files,
        format_func=lambda x: os.path.basename(x),
        help="Select the file you want to edit"
    )

    if selected_file:
        try:
            # Load file
            df = s3.get_file(selected_file)

            # Display file information
            st.info(f"File contains {len(df):,} rows and {len(df.columns):,} columns")

            # Create editor container
            with st.container():
                st.subheader("Edit Data")
                st.caption("Make your changes directly in the table below")

                # Pagination for editor
                rows_per_page = st.slider("Rows per page", 5, 50, 10)
                page = st.number_input("Page", 1, (len(df) // rows_per_page) + 1, 1)
                start_idx = (page - 1) * rows_per_page
                end_idx = min(start_idx + rows_per_page, len(df))

                # Data editor
                edited_df = st.data_editor(
                    df.iloc[start_idx:end_idx],
                    num_rows="dynamic",
                    use_container_width=True,
                    key="editor"
                )

                # Save changes button
                if st.button("Save Changes", type="primary"):
                    # Validate changes
                    is_valid, issues = validate_schema_match(edited_df, schema)

                    if not is_valid:
                        st.error("❌ Validation failed:")
                        for issue in issues:
                            st.warning(f"• {issue}")
                        return

                    try:
                        # Backup existing file
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        backup_path = f"{schema_name.lower()}/{table_name.lower()}/archive/{table_name.lower()}_{timestamp}.csv"
                        s3.rename_file(selected_file, backup_path)

                        # Save changes
                        csv_buffer = StringIO()
                        edited_df.to_csv(csv_buffer, index=False)
                        s3.upload_file(
                            BytesIO(csv_buffer.getvalue().encode()),
                            selected_file
                        )

                        st.success("✨ Changes saved successfully!")
                        st.info("📁 Previous version backed up with timestamp")

                    except Exception as e:
                        st.error(f"Save failed: {str(e)}")

                    try:
                        session.sql(f"ALTER EXTERNAL TABLE {table_name.lower()} REFRESH").collect()
                        st.success("External table refreshed successfully!")
                    except Exception as e:
                        st.error(f"Failed to refresh external table: {e}")

                # Show current page info
                st.caption(f"Showing rows {start_idx + 1} to {end_idx} of {len(df)}")

        except Exception as e:
            st.error(f"Error loading file: {str(e)}")


class S3Handler:
    def __init__(self, stage_name: str = "@streamlit_stage"):
        self.session = get_active_session()
        self.stage = stage_name
        self.stage_base_url = "s3://shariq-snowflake-streamlit/"

    def upload_file(self, file_obj, filename):
        if hasattr(file_obj, 'seek'):
            file_obj.seek(0)
        self.session.file.put_stream(file_obj, f"{self.stage}/{filename}", auto_compress=False, overwrite=True)
        return f"{self.stage}/{filename}"

    def get_file(self, filename, file_type="csv"):
        if self.file_exists(filename):
            with tempfile.TemporaryDirectory() as tmpdir:
                self.session.file.get(f"{self.stage}/{filename}", tmpdir)
                base_filename = os.path.basename(filename)
                local_path = os.path.join(tmpdir, base_filename)
                if file_type == "csv":
                    return pd.read_csv(local_path)
                elif file_type == "json":
                    with open(local_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
        else:
            return None

    def file_exists(self, filename):
        result = self.session.sql(f"LIST {self.stage}/{filename}").collect()
        return True if len(result) > 0 else False

    def rename_file(self, old_name, new_name):
        try:
            df = self.get_file(old_name)
    
            # Create full path in temp dir with your chosen name
            temp_dir = tempfile.gettempdir()
            temp_path = os.path.join(temp_dir, new_name)

            # Ensure the parent directory exists
            os.makedirs(os.path.dirname(temp_path), exist_ok=True)
            
    
            if new_name.endswith(".csv"):
                df.to_csv(temp_path, index=False)
            else:
                raise ValueError("Unsupported file type")


            upload_path = os.path.dirname(new_name)
    
            self.session.file.put(
                temp_path,
                f"{self.stage}/{upload_path}",
                auto_compress=False,
                overwrite=True
            )
    
            os.remove(temp_path)
    
        except Exception as e:
            st.error(f"Error renaming file: {str(e)}")

    def list_files(self, prefix=''):
        """List relative file paths in the S3 stage."""
        try:
            files = self.session.sql(f"LIST {self.stage}/{prefix}").collect()
            return [
                file["name"].replace(self.stage_base_url, "").lstrip("/")
                for file in files
            ]
        except Exception as e:
            st.error(f"Error listing files: {str(e)}")
            return []


def load_data(filename):
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_data(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)


def validate_dataframe(df: pd.DataFrame, schema: Dict) -> Tuple[bool, List[str]]:
    """
    Validate DataFrame against schema definition.
    Returns (is_valid, list_of_issues)
    """
    issues = []

    # Check for schema compatibility
    for col_name, col_info in schema['columns'].items():
        # Check if required column exists
        if col_name not in df.columns:
            issues.append(f"Missing required column: {col_name}")
            continue

        # Get column data
        col_data = df[col_name]

        # Check nulls
        if not col_info.get('nullable', False) and col_data.isnull().any():
            issues.append(f"Column '{col_name}' contains null values which are not allowed")

        # Validate data types
        try:
            if col_info['type'] == 'INTEGER':
                non_null_data = col_data.dropna()
                if not non_null_data.apply(lambda x: str(x).isdigit()).all():
                    issues.append(f"Column '{col_name}' contains non-integer values")

            elif col_info['type'] == 'FLOAT':
                pd.to_numeric(col_data.dropna(), errors='raise')

            elif col_info['type'] == 'DATETIME':
                pd.to_datetime(col_data.dropna(), errors='raise')

        except Exception as e:
            issues.append(f"Data type validation failed for column '{col_name}': {str(e)}")

    return len(issues) == 0, issues


def brand_finance_page():

    file_upload = st.sidebar.checkbox("File Upload", value=False)
    
    if file_upload:
        upload_page()
    
    manage_files = st.sidebar.checkbox("Manage Files", value=False)

    if manage_files:
        manage_files_page()

    # Add logout button
    st.sidebar.markdown("---")
    if st.sidebar.button("Logout"):
        st.session_state.show_main_app = False
        st.session_state.persona = None
        st.rerun()

    # Sidebar footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("#### Version 1.0.0")
    st.sidebar.markdown("NCLH Revenue Support Data Portal")
    

def select_persona():
    """Render persona selection page and return selected persona."""

    # Add some descriptive text
    st.sidebar.title("""
    **NCLH Revenue Support Data Portal**
    """)

    st.sidebar.markdown("""
    <div class="card">
        <h4>Brand Finance</h4>
        <ul>
            <li>Upload Data Files</li>
            <li>Manage And Edit Files</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    

    # Create a radio button for persona selection
    st.subheader("Select Your Role")
    persona = st.radio(
        "",
        ["Brand Finance"],
        key="persona_selection",
        horizontal=True
    )

    if st.button("Continue"):
        # Store the selected persona in session state
        st.session_state.persona = persona
        st.session_state.show_main_app = True
        st.rerun()


def main():
    """Main application entry point."""

    # Set page config
    st.set_page_config(
        page_title="NCLH Revenue Support Data Portal",
        page_icon="🚢",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.sidebar.image("https://companieslogo.com/img/orig/NCLH-3a0e162b.png?t=1740421112", width=100)

    # Add custom CSS
    st.markdown("""
        <style>
        .main .block-container {
            padding-top: 1rem;
            padding-bottom: 1rem;
        }
        .element-container {
            margin-bottom: 1rem;
        }
    
        /* Card styling */
        .card {
            padding: 15px 25px;
            margin: 10px 0;
            background-color: #f5f5f5;
            border-left: 5px solid #003366;
            border-radius: 10px;
            box-shadow: 2px 2px 8px rgba(0,0,0,0.1);
        }
        .card h4 {
            margin-bottom: 10px;
            color: #003366;
        }
        .card ul {
            margin: 0;
            padding-left: 20px;
        }
    
        /* Make all headings dark blue/navy */
        h1, h2, h3, h4, h5, h6 {
            color: #003366;
        }

        .stRadio > div {
            display: flex;
            gap: 1.5rem;
            flex-direction: row;
        }
        
        /* Radio as box */
        label[data-baseweb="radio"] {
            background-color: #f0f2f6;
            padding: 1rem 2rem;
            border: 2px solid transparent;
            border-radius: 10px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            transform: scale(1);
        }
        
        /* Hover effect */
        label[data-baseweb="radio"]:hover {
            border-color: #003366;
            transform: scale(1.03);
            box-shadow: 0 4px 12px rgba(0, 51, 102, 0.2);
        }

        /* Change outer ring of radio dot when selected */
        label[data-baseweb="radio"] > div:first-child {
            border: 2px solid #003366 !important;
            background-color: #003366;
        }

        
        .stButton>button {
            background-color: #003366;
            color: white;
            font-weight: bold;
            padding: 0.5em 2em;
            border-radius: 8px;
            border: none;
            transition: all 0.3s ease;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            width: 100%;
        }
        
        .stButton>button:hover {
            background-color: #0059b3;  /* Richer navy hover tone */
            color: white;
            transform: scale(1.03);     /* Subtle zoom */
            box-shadow: 0 4px 12px rgba(0, 51, 102, 0.3);  /* Deep navy shadow */
        }

        /* ✅ Label text styling */
        [data-testid="stCheckbox"] div[data-testid="stMarkdownContainer"] {
            font-size: 20px !important;
            font-weight: 600 !important;
            font-family: 'Segoe UI', sans-serif !important;
        }
    
        /* ✅ Resize checkbox box and style tick color */
        [data-testid="stCheckbox"] input[type="checkbox"] {
            transform: scale(1.6);
            accent-color: #003366 !important;
            cursor: pointer;
            transition: all 0.3s ease;
        }
    
        /* ✅ Base card-like style */
        [data-testid="stCheckbox"] {
            padding: 12px 16px;
            border-radius: 10px;
            background-color: #f0f2f6;
            transition: all 0.3s ease;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            display: flex;
            align-items: center;
        }
    
        /* ✅ Hover effect */
        [data-testid="stCheckbox"]:hover {
            background-color: #f0f2f6;
            transform: scale(1.03);
            box-shadow: 0 4px 12px rgba(0, 51, 102, 0.2);  
        }
    
        /* ✅ When checkbox is selected, apply hover-like effect to the full container */
        [data-testid="stCheckbox"] input[type="checkbox"]:checked ~ div[data-testid="stMarkdownContainer"] {
            background-color: #f0f8ff;
            box-shadow: 0 4px 12px rgba(0, 51, 102, 0.2);
            border-radius: 10px;
            padding: 0.2rem 0.5rem;
        }

        </style>
    """, unsafe_allow_html=True)


    # Initialize session state
    if 'show_main_app' not in st.session_state:
        st.session_state.show_main_app = False

    if 'persona' not in st.session_state:
        st.session_state.persona = None

    # Show persona selection if no persona is selected
    if not st.session_state.show_main_app:
        select_persona()
        return

    # Sidebar navigation
    team = "👤" + st.session_state.persona
    st.sidebar.title(team)

    # Different navigation options based on persona
    if st.session_state.persona == "Brand Finance":
        brand_finance_page()


if __name__ == "__main__":
    main()
 