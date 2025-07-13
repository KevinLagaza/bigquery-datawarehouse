from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pathlib import Path
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.decorators import dag, task, task_group

GCP_CONN_ID = Variable.get("GCP_CONN_ID", default_var="<default_variable_mentioned_in_airflow_connection>") # Your BigQuery Airflow connection ID
hook_bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
hook_gcs = GCSHook(gcp_conn_id=GCP_CONN_ID)
project_id='ecstatic-night-457507-v7'
service_account = "dbt-user-dev@ecstatic-night-457507-v7.iam.gserviceaccount.com"
sql_folder = '/usr/local/airflow/sql'

datasets = ['bronze', 'silver', 'gold']

def read_sql_file(file_path:Path):
    with open(file_path, 'r') as file:
        return file.read()

@dag(
        start_date=datetime(2025, 5, 8),
        schedule=None,
        template_searchpath=[f'{sql_folder}/']
    )
def create_bigquery_dwh_dag():
    
    @task_group
    def create_datasets():
        from airflow.exceptions import AirflowFailException
        success = True
        for dataset in datasets:
            try:
                print(f"Creation du dataset: {dataset}")
                BigQueryCreateEmptyDatasetOperator(
                    task_id=f'create_{dataset}',
                    dataset_id=f'{dataset}',
                    location='US',
                    project_id=project_id,
                    gcp_conn_id=GCP_CONN_ID,
                    exists_ok=True,  # Skip if dataset exists
                )
            except Exception as e:
                print(f"Echec de la creation du dataset {dataset}: {str(e)}")
                success = False
        if not success:
            raise AirflowFailException("Dataset creation failed")

    @task_group
    def load_data_into_bronze():
        """
            Chargement des données dans la zone Bronze
        """

        create_tables_in_bronze = BigQueryInsertJobOperator(
            task_id=f'create_initial_tables_in_bronze',
            configuration={
                "query": {
                    "query": read_sql_file(Path(f"{sql_folder}/bronze/ddl_bronze_table.sql")),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        load_csv_into_bronze_crm_cust_info = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_crm_cust_info',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['crm/cust_info.csv'],
            source_format='CSV',
            destination_project_dataset_table=f"{project_id}.bronze.crm_cust_info",
            project_id=project_id,
            field_delimiter=',',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED', 
            impersonation_chain=[f"{service_account}"],
            skip_leading_rows=1,
            write_disposition='WRITE_EMPTY',
            gcp_conn_id=GCP_CONN_ID,           
        )

        load_csv_into_bronze_crm_prd_info = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_crm_prd_info',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['crm/prd_info.csv'],
            source_format='CSV',
            destination_project_dataset_table=f"{project_id}.bronze.crm_prd_info",
            field_delimiter=',',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED',
            impersonation_chain=[f"{service_account}"],
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
        )

        load_csv_into_bronze_crm_sales_details = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_crm_sales_details',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['crm/sales_details.csv'],
            source_format='CSV',
            destination_project_dataset_table=f"{project_id}.bronze.crm_sales_details",
            field_delimiter=',',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED',
            impersonation_chain=[f"{service_account}"],
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
        )

        load_csv_into_bronze_erp_px_cat_g1v2 = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_erp_px_cat_g1v2',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['erp/PX_CAT_G1V2.csv'],
            source_format='CSV',
            destination_project_dataset_table=f"{project_id}.bronze.erp_px_cat_g1v2",
            field_delimiter=',',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED',
            impersonation_chain=[f"{service_account}"],
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
        )

        load_csv_into_bronze_erp_loc_a101 = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_erp_loc_a101',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['erp/LOC_A101.csv'],
            source_format='CSV',
            destination_project_dataset_table=f"{project_id}-v7.bronze.erp_loc_a101",
            field_delimiter=',',
            create_disposition='CREATE_IF_NEEDED',
            allow_quoted_newlines=True,
            impersonation_chain=[f"{service_account}"],
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
            schema_fields=[
                {'name': 'cid', 'type': 'STRING'},
                {'name': 'cntry', 'type': 'STRING', 'mode': 'NULLABLE'},
            ],
        )

        load_csv_into_bronze_erp_cust_az12 = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_erp_cust_az12',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['erp/CUST_AZ12.csv'],
            source_format='CSV',
            destination_project_dataset_table=f"{project_id}.bronze.erp_cust_az12",
            field_delimiter = ',',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED',
            impersonation_chain=["f{service_account}"],
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
        )
    
        create_tables_in_bronze >> [load_csv_into_bronze_crm_cust_info, load_csv_into_bronze_crm_prd_info, load_csv_into_bronze_crm_sales_details, load_csv_into_bronze_erp_px_cat_g1v2, load_csv_into_bronze_erp_loc_a101, load_csv_into_bronze_erp_cust_az12]

    @task_group
    def load_data_into_silver():
        """
            Chargement des données dans la zone Silver
        """

        create_tables_in_silver = BigQueryInsertJobOperator(
            task_id=f'create_initial_tables_in_silver',
                configuration={
                "query": {
                    "query": read_sql_file(Path("{sql_folder}/silver/ddl_silver_table.sql")),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        create_procedure_to_insert_data_in_silver = BigQueryInsertJobOperator(
            task_id=f'create_procedure_to_insert_data_in_silver',
                configuration={
                "query": {
                    "query": read_sql_file(Path(f"{sql_folder}/silver/dml_silver_table.sql")),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        run_procedure_to_insert_data_in_silver = BigQueryInsertJobOperator(
            task_id='run_procedure_to_insert_data_in_silver',
            configuration={
                "query": {
                    "query": f"""
                        CALL `{project_id}.silver.load_silver`(
                        CURRENT_DATE()  -- Airflow execution date
                        );
                    """,
                    "useLegacySql": False
                }
            },
            gcp_conn_id=GCP_CONN_ID
        )
        create_tables_in_silver >> create_procedure_to_insert_data_in_silver >> run_procedure_to_insert_data_in_silver
    
    @task_group
    def load_data_into_gold():
        """
            Chargement des données dans la zone Gold
        """

        create_dim_tables_in_gold = BigQueryInsertJobOperator(
            task_id=f'create_dim_tables_in_gold',
                configuration={
                "query": {
                    "query": read_sql_file(Path(f"{sql_folder}/gold/dml_gold_dimensions.sql")),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        create_facts_tables_in_gold = BigQueryInsertJobOperator(
            task_id=f'create_facts_tables_in_gold',
                configuration={
                "query": {
                    "query": read_sql_file(Path(f"{sql_folder}/gold/dml_gold_facts.sql")),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        create_dim_tables_in_gold >> create_facts_tables_in_gold

    create_datasets() >> load_data_into_bronze() >> load_data_into_silver() >> load_data_into_gold()

dag = create_bigquery_dwh_dag()



        


    
