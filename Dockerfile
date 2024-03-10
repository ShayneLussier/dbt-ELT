FROM apache/airflow:2.8.1

# Install additional Python packages
RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Create a user named "airflow" with UID and GID specified by the AIRFLOW_UID environment variable
USER root
ARG AIRFLOW_UID=1000
RUN id -u airflow &>/dev/null || adduser --uid $AIRFLOW_UID --disabled-password --gecos '' airflow

# Set the working directory
WORKDIR /opt/airflow

# Copy DAGs folder
# COPY --chown=airflow:root ./dags /opt/airflow/dags

# Switch to the "airflow" user
USER airflow