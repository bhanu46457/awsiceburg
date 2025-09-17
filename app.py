import streamlit as st
import boto3
import json
import pandas as pd
from datetime import datetime
import time
from typing import Dict, List, Any, Optional
import os

# Page configuration
st.set_page_config(
    page_title="AWS Glue to Iceberg Migration Tool",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for gradient styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
    }
    
    .section-header {
        background: linear-gradient(90deg, #f093fb 0%, #f5576c 100%);
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        color: white;
        font-weight: bold;
    }
    
    .info-box {
        background: linear-gradient(90deg, #4facfe 0%, #00f2fe 100%);
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        color: white;
    }
    
    .success-box {
        background: linear-gradient(90deg, #43e97b 0%, #38f9d7 100%);
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        color: white;
    }
    
    .warning-box {
        background: linear-gradient(90deg, #fa709a 0%, #fee140 100%);
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        color: white;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem;
    }
    
    .stSelectbox > div > div {
        background-color: #f0f2f6;
        border-radius: 5px;
    }
    
    .stTextInput > div > div > input {
        background-color: #f0f2f6;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'step' not in st.session_state:
    st.session_state.step = 1
if 'selected_database' not in st.session_state:
    st.session_state.selected_database = None
if 'selected_table' not in st.session_state:
    st.session_state.selected_table = None
if 'table_metadata' not in st.session_state:
    st.session_state.table_metadata = None
if 'config_created' not in st.session_state:
    st.session_state.config_created = False

class GlueIcebergMigrator:
    def __init__(self):
        self.glue_client = boto3.client('glue')
        self.s3_client = boto3.client('s3')
        self.athena_client = boto3.client('athena')
        
    def get_databases(self) -> List[str]:
        """Get list of databases from Glue catalog"""
        try:
            response = self.glue_client.get_databases()
            return [db['Name'] for db in response['DatabaseList']]
        except Exception as e:
            st.error(f"Error fetching databases: {str(e)}")
            return []
    
    def get_tables(self, database_name: str) -> List[str]:
        """Get list of tables from specified database"""
        try:
            response = self.glue_client.get_tables(DatabaseName=database_name)
            return [table['Name'] for table in response['TableList']]
        except Exception as e:
            st.error(f"Error fetching tables: {str(e)}")
            return []
    
    def get_table_metadata(self, database_name: str, table_name: str) -> Dict[str, Any]:
        """Get detailed metadata for a table"""
        try:
            response = self.glue_client.get_table(DatabaseName=database_name, Name=table_name)
            table = response['Table']
            
            metadata = {
                's3_location': table.get('StorageDescriptor', {}).get('Location', 'N/A'),
                'input_format': table.get('StorageDescriptor', {}).get('InputFormat', 'N/A'),
                'output_format': table.get('StorageDescriptor', {}).get('OutputFormat', 'N/A'),
                'serde_info': table.get('StorageDescriptor', {}).get('SerdeInfo', {}),
                'columns': table.get('StorageDescriptor', {}).get('Columns', []),
                'partition_keys': table.get('PartitionKeys', []),
                'table_type': table.get('TableType', 'EXTERNAL_TABLE'),
                'parameters': table.get('Parameters', {}),
                'create_time': table.get('CreateTime', ''),
                'update_time': table.get('UpdateTime', '')
            }
            
            return metadata
        except Exception as e:
            st.error(f"Error fetching table metadata: {str(e)}")
            return {}
    
    def get_default_data_type_mapping(self) -> Dict[str, str]:
        """Get default mapping from Glue data types to Iceberg data types"""
        return {
            'string': 'string',
            'varchar': 'string',
            'char': 'string',
            'text': 'string',
            'int': 'int',
            'integer': 'int',
            'bigint': 'long',
            'long': 'long',
            'smallint': 'int',
            'tinyint': 'int',
            'float': 'float',
            'double': 'double',
            'decimal': 'decimal',
            'boolean': 'boolean',
            'bool': 'boolean',
            'date': 'date',
            'timestamp': 'timestamp',
            'binary': 'binary',
            'array': 'array',
            'map': 'map',
            'struct': 'struct'
        }
    
    def create_config_file(self, config_data: Dict[str, Any], s3_bucket: str, s3_key: str) -> bool:
        """Create and upload configuration file to S3"""
        try:
            config_json = json.dumps(config_data, indent=2, default=str)
            
            self.s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=config_json,
                ContentType='application/json'
            )
            return True
        except Exception as e:
            st.error(f"Error creating config file: {str(e)}")
            return False
    
    def execute_glue_job(self, job_name: str, config_s3_path: str) -> str:
        """Execute Glue job with configuration"""
        try:
            response = self.glue_client.start_job_run(
                JobName=job_name,
                Arguments={
                    '--config-s3-path': config_s3_path,
                    '--datalake-formats': 'iceberg'
                }
            )
            return response['JobRunId']
        except Exception as e:
            st.error(f"Error starting Glue job: {str(e)}")
            return None

def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>❄️ AWS Glue to Iceberg Migration Tool</h1>
        <p>Migrate your AWS Glue catalog tables to Apache Iceberg using Glue 5.0</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize migrator
    migrator = GlueIcebergMigrator()
    
    # Sidebar for navigation
    with st.sidebar:
        st.markdown("### 🧭 Migration Steps")
        steps = [
            "1. Select Source Database & Table",
            "2. Review Table Metadata",
            "3. Configure Data Type Mapping",
            "4. Set Target Configuration",
            "5. Configure Iceberg Settings",
            "6. Generate & Execute Migration"
        ]
        
        for i, step in enumerate(steps, 1):
            if i == st.session_state.step:
                st.markdown(f"**{step}** ✅")
            else:
                st.markdown(step)
        
        st.markdown("---")
        st.markdown("### 📊 Migration Progress")
        progress = st.session_state.step / 6
        st.progress(progress)
        st.markdown(f"**{int(progress * 100)}% Complete**")
    
    # Main content area
    if st.session_state.step == 1:
        step1_database_table_selection(migrator)
    elif st.session_state.step == 2:
        step2_table_metadata_review(migrator)
    elif st.session_state.step == 3:
        step3_data_type_mapping(migrator)
    elif st.session_state.step == 4:
        step4_target_configuration(migrator)
    elif st.session_state.step == 5:
        step5_iceberg_settings(migrator)
    elif st.session_state.step == 6:
        step6_execute_migration(migrator)

def step1_database_table_selection(migrator):
    st.markdown('<div class="section-header">📁 Step 1: Select Source Database & Table</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🗄️ Select Database")
        databases = migrator.get_databases()
        
        if databases:
            selected_db = st.selectbox(
                "Choose a database:",
                options=databases,
                index=0 if not st.session_state.selected_database else databases.index(st.session_state.selected_database)
            )
            st.session_state.selected_database = selected_db
            
            if selected_db:
                st.markdown(f"**Selected Database:** {selected_db}")
                
                with col2:
                    st.markdown("### 📋 Select Table")
                    tables = migrator.get_tables(selected_db)
                    
                    if tables:
                        selected_table = st.selectbox(
                            "Choose a table:",
                            options=tables,
                            index=0 if not st.session_state.selected_table else tables.index(st.session_state.selected_table) if st.session_state.selected_table in tables else 0
                        )
                        st.session_state.selected_table = selected_table
                        
                        if selected_table:
                            st.markdown(f"**Selected Table:** {selected_table}")
                            
                            if st.button("🔍 Analyze Table", type="primary"):
                                with st.spinner("Fetching table metadata..."):
                                    st.session_state.table_metadata = migrator.get_table_metadata(selected_db, selected_table)
                                    st.session_state.step = 2
                                    st.rerun()
                    else:
                        st.warning("No tables found in the selected database.")
        else:
            st.error("No databases found. Please check your AWS credentials and Glue catalog.")

def step2_table_metadata_review(migrator):
    st.markdown('<div class="section-header">📊 Step 2: Review Table Metadata</div>', unsafe_allow_html=True)
    
    if not st.session_state.table_metadata:
        st.error("No table metadata available. Please go back to step 1.")
        if st.button("← Back to Step 1"):
            st.session_state.step = 1
            st.rerun()
        return
    
    metadata = st.session_state.table_metadata
    
    # Display metadata in cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card">📍 S3 Location</div>', unsafe_allow_html=True)
        st.text(metadata.get('s3_location', 'N/A'))
    
    with col2:
        st.markdown('<div class="metric-card">📄 Input Format</div>', unsafe_allow_html=True)
        st.text(metadata.get('input_format', 'N/A').split('.')[-1] if metadata.get('input_format') != 'N/A' else 'N/A')
    
    with col3:
        st.markdown('<div class="metric-card">🔧 Table Type</div>', unsafe_allow_html=True)
        st.text(metadata.get('table_type', 'N/A'))
    
    with col4:
        st.markdown('<div class="metric-card">📅 Created</div>', unsafe_allow_html=True)
        if metadata.get('create_time'):
            st.text(metadata['create_time'].strftime('%Y-%m-%d'))
        else:
            st.text('N/A')
    
    # Detailed information
    st.markdown("### 📋 Table Details")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🏷️ Columns")
        if metadata.get('columns'):
            columns_df = pd.DataFrame(metadata['columns'])
            st.dataframe(columns_df, use_container_width=True)
        else:
            st.info("No columns found")
    
    with col2:
        st.markdown("#### 🔑 Partition Keys")
        if metadata.get('partition_keys'):
            partitions_df = pd.DataFrame(metadata['partition_keys'])
            st.dataframe(partitions_df, use_container_width=True)
        else:
            st.info("No partition keys found")
    
    # Serde information
    if metadata.get('serde_info'):
        st.markdown("#### ⚙️ Serde Information")
        serde_info = metadata['serde_info']
        st.json(serde_info)
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("← Back to Step 1"):
            st.session_state.step = 1
            st.rerun()
    
    with col3:
        if st.button("Next: Data Type Mapping →", type="primary"):
            st.session_state.step = 3
            st.rerun()

def step3_data_type_mapping(migrator):
    st.markdown('<div class="section-header">🔄 Step 3: Configure Data Type Mapping</div>', unsafe_allow_html=True)
    
    if not st.session_state.table_metadata:
        st.error("No table metadata available. Please go back to step 1.")
        return
    
    metadata = st.session_state.table_metadata
    columns = metadata.get('columns', [])
    
    if not columns:
        st.warning("No columns found in the table.")
        if st.button("← Back to Step 2"):
            st.session_state.step = 2
            st.rerun()
        return
    
    st.markdown("### 🗺️ Data Type Mapping Configuration")
    st.info("Review and modify the data type mappings from Glue to Iceberg format. Default mappings are provided based on best practices.")
    
    # Initialize data type mapping in session state
    if 'data_type_mapping' not in st.session_state:
        default_mapping = migrator.get_default_data_type_mapping()
        st.session_state.data_type_mapping = {}
        
        for column in columns:
            glue_type = column.get('Type', 'string').lower()
            # Find the best match from default mapping
            iceberg_type = default_mapping.get(glue_type, 'string')
            st.session_state.data_type_mapping[column['Name']] = {
                'glue_type': column.get('Type', 'string'),
                'iceberg_type': iceberg_type
            }
    
    # Display mapping interface
    st.markdown("#### 📝 Column Data Type Mappings")
    
    mapping_data = []
    for column_name, mapping in st.session_state.data_type_mapping.items():
        mapping_data.append({
            'Column Name': column_name,
            'Glue Type': mapping['glue_type'],
            'Iceberg Type': mapping['iceberg_type']
        })
    
    mapping_df = pd.DataFrame(mapping_data)
    
    # Create editable mapping interface
    for i, row in mapping_df.iterrows():
        col1, col2, col3 = st.columns([2, 2, 2])
        
        with col1:
            st.text_input(f"Column {i+1}", value=row['Column Name'], disabled=True, key=f"col_name_{i}")
        
        with col2:
            st.text_input(f"Glue Type {i+1}", value=row['Glue Type'], disabled=True, key=f"glue_type_{i}")
        
        with col3:
            iceberg_options = ['string', 'int', 'long', 'float', 'double', 'boolean', 'date', 'timestamp', 'binary', 'decimal', 'array', 'map', 'struct']
            current_iceberg = row['Iceberg Type']
            selected_iceberg = st.selectbox(
                f"Iceberg Type {i+1}",
                options=iceberg_options,
                index=iceberg_options.index(current_iceberg) if current_iceberg in iceberg_options else 0,
                key=f"iceberg_type_{i}"
            )
            st.session_state.data_type_mapping[row['Column Name']]['iceberg_type'] = selected_iceberg
    
    # Show summary
    st.markdown("#### 📊 Mapping Summary")
    updated_mapping_df = pd.DataFrame([
        {
            'Column Name': name,
            'Glue Type': mapping['glue_type'],
            'Iceberg Type': mapping['iceberg_type']
        }
        for name, mapping in st.session_state.data_type_mapping.items()
    ])
    st.dataframe(updated_mapping_df, use_container_width=True)
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("← Back to Step 2"):
            st.session_state.step = 2
            st.rerun()
    
    with col3:
        if st.button("Next: Target Configuration →", type="primary"):
            st.session_state.step = 4
            st.rerun()

def step4_target_configuration(migrator):
    st.markdown('<div class="section-header">🎯 Step 4: Set Target Configuration</div>', unsafe_allow_html=True)
    
    st.markdown("### 🏗️ Target Table Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📊 Basic Configuration")
        target_database = st.text_input(
            "Target Database Name",
            value=f"{st.session_state.selected_database}_iceberg",
            help="Name of the target database for the Iceberg table"
        )
        
        target_table = st.text_input(
            "Target Table Name",
            value=f"{st.session_state.selected_table}_iceberg",
            help="Name of the target Iceberg table"
        )
        
        target_s3_location = st.text_input(
            "Target S3 Location",
            value=f"s3://your-bucket/iceberg-tables/{target_database}/{target_table}/",
            help="S3 location where the Iceberg table will be stored"
        )
    
    with col2:
        st.markdown("#### ⚙️ Table Properties")
        table_description = st.text_area(
            "Table Description",
            value=f"Migrated Iceberg table from {st.session_state.selected_database}.{st.session_state.selected_table}",
            help="Description for the target table"
        )
        
        owner = st.text_input(
            "Table Owner",
            value="data-engineering",
            help="Owner of the target table"
        )
    
    # Store configuration in session state
    st.session_state.target_config = {
        'target_database': target_database,
        'target_table': target_table,
        'target_s3_location': target_s3_location,
        'table_description': table_description,
        'owner': owner
    }
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("← Back to Step 3"):
            st.session_state.step = 3
            st.rerun()
    
    with col3:
        if st.button("Next: Iceberg Settings →", type="primary"):
            st.session_state.step = 5
            st.rerun()

def step5_iceberg_settings(migrator):
    st.markdown('<div class="section-header">❄️ Step 5: Configure Iceberg Settings</div>', unsafe_allow_html=True)
    
    st.markdown("### ⚙️ Iceberg Configuration Parameters")
    st.info("Configure advanced Iceberg settings for optimal performance and functionality.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🚀 Performance Settings")
        
        # File format
        file_format = st.selectbox(
            "File Format",
            options=['parquet', 'orc', 'avro'],
            index=0,
            help="File format for Iceberg table storage"
        )
        
        # Compression
        compression = st.selectbox(
            "Compression",
            options=['snappy', 'gzip', 'lz4', 'zstd', 'none'],
            index=0,
            help="Compression algorithm for data files"
        )
        
        # Target file size
        target_file_size = st.number_input(
            "Target File Size (MB)",
            min_value=1,
            max_value=1024,
            value=128,
            help="Target size for data files in MB"
        )
        
        # Write target file size
        write_target_file_size = st.number_input(
            "Write Target File Size (MB)",
            min_value=1,
            max_value=1024,
            value=64,
            help="Target size for write operations in MB"
        )
    
    with col2:
        st.markdown("#### 🔧 Advanced Settings")
        
        # Compaction settings
        compaction_enabled = st.checkbox(
            "Enable Automatic Compaction",
            value=True,
            help="Enable automatic compaction for the table"
        )
        
        compaction_target_size = st.number_input(
            "Compaction Target Size (MB)",
            min_value=1,
            max_value=10240,
            value=512,
            help="Target size for compaction operations"
        )
        
        # History retention
        history_retention_days = st.number_input(
            "History Retention (Days)",
            min_value=1,
            max_value=365,
            value=30,
            help="Number of days to retain table history"
        )
        
        # Snapshot retention
        snapshot_retention_days = st.number_input(
            "Snapshot Retention (Days)",
            min_value=1,
            max_value=365,
            value=7,
            help="Number of days to retain snapshots"
        )
    
    # Additional settings
    st.markdown("#### 🎛️ Additional Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Catalog settings
        catalog_name = st.text_input(
            "Catalog Name",
            value="glue_catalog",
            help="Name of the Iceberg catalog"
        )
        
        # Warehouse location
        warehouse_location = st.text_input(
            "Warehouse Location",
            value=st.session_state.target_config.get('target_s3_location', 's3://your-bucket/warehouse/'),
            help="S3 location for the Iceberg warehouse"
        )
    
    with col2:
        # Glue job settings
        glue_job_name = st.text_input(
            "Glue Job Name",
            value=f"iceberg-migration-{st.session_state.selected_table}",
            help="Name of the Glue job for migration"
        )
        
        # Config file S3 location
        config_s3_bucket = st.text_input(
            "Config S3 Bucket",
            value="your-config-bucket",
            help="S3 bucket for storing configuration files"
        )
        
        config_s3_key = st.text_input(
            "Config S3 Key",
            value=f"configs/iceberg-migration-{st.session_state.selected_table}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json",
            help="S3 key for the configuration file"
        )
    
    # Store Iceberg settings in session state
    st.session_state.iceberg_settings = {
        'file_format': file_format,
        'compression': compression,
        'target_file_size': target_file_size,
        'write_target_file_size': write_target_file_size,
        'compaction_enabled': compaction_enabled,
        'compaction_target_size': compaction_target_size,
        'history_retention_days': history_retention_days,
        'snapshot_retention_days': snapshot_retention_days,
        'catalog_name': catalog_name,
        'warehouse_location': warehouse_location,
        'glue_job_name': glue_job_name,
        'config_s3_bucket': config_s3_bucket,
        'config_s3_key': config_s3_key
    }
    
    # Show configuration summary
    st.markdown("#### 📋 Configuration Summary")
    
    config_summary = {
        "Source": f"{st.session_state.selected_database}.{st.session_state.selected_table}",
        "Target": f"{st.session_state.target_config['target_database']}.{st.session_state.target_config['target_table']}",
        "File Format": file_format,
        "Compression": compression,
        "Compaction": "Enabled" if compaction_enabled else "Disabled",
        "Glue Job": glue_job_name
    }
    
    summary_df = pd.DataFrame(list(config_summary.items()), columns=['Setting', 'Value'])
    st.dataframe(summary_df, use_container_width=True)
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("← Back to Step 4"):
            st.session_state.step = 4
            st.rerun()
    
    with col3:
        if st.button("Next: Execute Migration →", type="primary"):
            st.session_state.step = 6
            st.rerun()

def step6_execute_migration(migrator):
    st.markdown('<div class="section-header">🚀 Step 6: Execute Migration</div>', unsafe_allow_html=True)
    
    st.markdown("### 🎯 Migration Execution")
    
    # Show final configuration
    st.markdown("#### 📋 Final Configuration Review")
    
    config_data = {
        "source": {
            "database": st.session_state.selected_database,
            "table": st.session_state.selected_table,
            "metadata": st.session_state.table_metadata
        },
        "target": st.session_state.target_config,
        "data_type_mapping": st.session_state.data_type_mapping,
        "iceberg_settings": st.session_state.iceberg_settings,
        "migration_timestamp": datetime.now().isoformat()
    }
    
    # Display configuration
    st.json(config_data)
    
    # Execution section
    st.markdown("#### ⚡ Execute Migration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📄 Generate Config File", type="primary"):
            with st.spinner("Creating configuration file..."):
                success = migrator.create_config_file(
                    config_data,
                    st.session_state.iceberg_settings['config_s3_bucket'],
                    st.session_state.iceberg_settings['config_s3_key']
                )
                
                if success:
                    st.session_state.config_created = True
                    config_s3_path = f"s3://{st.session_state.iceberg_settings['config_s3_bucket']}/{st.session_state.iceberg_settings['config_s3_key']}"
                    
                    st.markdown(f"""
                    <div class="success-box">
                        <h4>✅ Configuration File Created Successfully!</h4>
                        <p><strong>S3 Location:</strong> {config_s3_path}</p>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.error("Failed to create configuration file.")
    
    with col2:
        if st.session_state.config_created:
            if st.button("🚀 Execute Glue Job", type="primary"):
                with st.spinner("Starting Glue job..."):
                    config_s3_path = f"s3://{st.session_state.iceberg_settings['config_s3_bucket']}/{st.session_state.iceberg_settings['config_s3_key']}"
                    job_run_id = migrator.execute_glue_job(
                        st.session_state.iceberg_settings['glue_job_name'],
                        config_s3_path
                    )
                    
                    if job_run_id:
                        st.markdown(f"""
                        <div class="success-box">
                            <h4>🚀 Glue Job Started Successfully!</h4>
                            <p><strong>Job Run ID:</strong> {job_run_id}</p>
                            <p><strong>Job Name:</strong> {st.session_state.iceberg_settings['glue_job_name']}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Show monitoring section
                        st.markdown("#### 📊 Job Monitoring")
                        st.info("You can monitor the job progress in the AWS Glue console or use the monitoring section below.")
                        
                        if st.button("🔄 Check Job Status"):
                            with st.spinner("Checking job status..."):
                                try:
                                    response = migrator.glue_client.get_job_run(
                                        JobName=st.session_state.iceberg_settings['glue_job_name'],
                                        RunId=job_run_id
                                    )
                                    
                                    job_run = response['JobRun']
                                    status = job_run['JobRunState']
                                    
                                    if status == 'SUCCEEDED':
                                        st.success(f"✅ Job completed successfully!")
                                        
                                        # Show Athena integration
                                        st.markdown("#### 🔍 Athena Integration")
                                        st.markdown(f"""
                                        <div class="info-box">
                                            <h4>🎉 Migration Complete!</h4>
                                            <p>Your Iceberg table is now available in Athena:</p>
                                            <p><strong>Database:</strong> {st.session_state.target_config['target_database']}</p>
                                            <p><strong>Table:</strong> {st.session_state.target_config['target_table']}</p>
                                        </div>
                                        """, unsafe_allow_html=True)
                                        
                                        # Sample Athena query
                                        sample_query = f"""
                                        SELECT * FROM {st.session_state.target_config['target_database']}.{st.session_state.target_config['target_table']} 
                                        LIMIT 10;
                                        """
                                        
                                        st.markdown("#### 📝 Sample Athena Query")
                                        st.code(sample_query, language='sql')
                                        
                                        if st.button("🔍 Open in Athena"):
                                            st.info("Please open the AWS Athena console to run queries on your new Iceberg table.")
                                    
                                    elif status == 'FAILED':
                                        st.error(f"❌ Job failed: {job_run.get('ErrorMessage', 'Unknown error')}")
                                    
                                    else:
                                        st.info(f"⏳ Job is {status.lower()}. Please wait for completion.")
                                        
                                except Exception as e:
                                    st.error(f"Error checking job status: {str(e)}")
                    else:
                        st.error("Failed to start Glue job.")
        else:
            st.warning("Please generate the configuration file first.")
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("← Back to Step 5"):
            st.session_state.step = 5
            st.rerun()
    
    with col2:
        if st.button("🔄 Start New Migration"):
            # Reset session state
            for key in ['step', 'selected_database', 'selected_table', 'table_metadata', 'config_created']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()

if __name__ == "__main__":
    main()
