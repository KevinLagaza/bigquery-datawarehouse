FROM quay.io/astronomer/astro-runtime:12.8.0

ENV VENV_PATH="/usr/local/airflow/dbt_venv"
ENV PATH="$VENV_PATH/bin:$PATH"

RUN python -m venv $VENV_PATH && \
    source $VENV_PATH/bin/activate && \
    pip install apache-airflow-providers-google==15.1.0 --no-deps
    

COPY <file_path_to_local_service_account_file> /usr/local/airflow/<file_path_in_container>
