#!/bin/bash

set -e

AIRFLOW_ADMIN_EMAIL="${AIRFLOW_ADMIN_EMAIL:-admin@example.com}"
AIRFLOW_DOCKERFILE="airflow.Dockerfile"
AIRFLOW_ENTRYPOINT_SCRIPT_PATH="airflow-entrypoint.sh"
AIRFLOW_ADMIN_FIRST_NAME="${AIRFLOW_ADMIN_FIRST_NAME:-Administrator}"
AIRFLOW_ADMIN_LAST_NAME="${AIRFLOW_ADMIN_LAST_NAME:-System}"
AIRFLOW_ADMIN_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:-admin}"
AIRFLOW_ADMIN_USERNAME="${AIRFLOW_ADMIN_USERNAME:-admin}"
AIRFLOW_WEBSERVER_SECRET_KEY="${AIRFLOW_WEBSERVER_SECRET_KEY:-airflowsecretkey}"

DOCKER_COMPOSE_FILE_PATH="docker-compose.yaml"
DOCKER_NETWORK_NAME="portfolio-analyzer-network"
DOCKER_NETWORK_SUBNET="192.168.0.0/24"

PORTFOLIO_ANALYZER_APPLICATION_PATH="portfolio-analyzer"
PORTFOLIO_ANALYZER_DOCKERFILE="portfolio-analyzer.Dockerfile"

POSTGRES_AIRFLOW_DATABASE="${POSTGRES_AIRFLOW_DATABASE:-airflow}"
POSTGRES_AIRFLOW_PASSWORD="${POSTGRES_AIRFLOW_PASSWORD:-airflowpassword}"
POSTGRES_AIRFLOW_USERNAME="${POSTGRES_AIRFLOW_USERNAME:-airflow}"
POSTGRES_INIT_SCRIPT_PATH="postgres-init.sql"
POSTGRES_USERNAME="${POSTGRES_USERNAME:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgrespassword}"
POSTGRES_PORTFOLIOANALYZER_DATABASE="${POSTGRES_PORTFOLIOANALYZER_DATABASE:-portfolioanalyzer}"
POSTGRES_PORTFOLIOANALYZER_PASSWORD="${POSTGRES_PORTFOLIOANALYZER_PASSWORD:-portfolioanalyzerdatabasepassword}"
POSTGRES_PORTFOLIOANALYZER_USERNAME="${POSTGRES_PORTFOLIOANALYZER_USERNAME:-portfolioanalyzer}"

GREEN='\033[0;32m'
NC='\033[0m'
RED='\033[0;31m'

echo_success() {
    echo -e "${GREEN}$1${NC}"
}

echo_error() {
    echo -e "${RED}$1${NC}"
}

for cmd in docker python3 pip; do
    if ! command -v $cmd &> /dev/null; then
        echo_error "Error: $cmd is not installed!"
        exit 1
    fi
done

echo_success "Creating Airflow Dockerfile"
cat <<EOF > "$AIRFLOW_DOCKERFILE"
FROM apache/airflow:latest

USER root
RUN apt-get update && apt-get install -y \\
    gcc \\
    libpq-dev \\
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir psycopg2-binary
EOF

echo_success "Creating entrypoint script for Airflow"
cat <<EOF > "$AIRFLOW_ENTRYPOINT_SCRIPT_PATH"
#!/bin/bash

echo "Initializing Airflow database"
airflow db migrate

echo "Creating Airflow admin user (if not exists)"
airflow users list | grep -q "$AIRFLOW_ADMIN_USERNAME" || \\
airflow users create \\
    --username $AIRFLOW_ADMIN_USERNAME \\
    --password $AIRFLOW_ADMIN_PASSWORD \\
    --firstname $AIRFLOW_ADMIN_FIRST_NAME \\
    --lastname $AIRFLOW_ADMIN_LAST_NAME \\
    --role Admin \\
    --email $AIRFLOW_ADMIN_EMAIL || echo "User already exists, skipping"

echo "Starting Airflow webserver"
exec airflow webserver

echo "Exiting to trigger restart via restart policy."
exit 0
EOF

chmod +x "$AIRFLOW_ENTRYPOINT_SCRIPT_PATH"

echo_success "Creating Portfolio Analyzer Dockerfile"
cat <<EOF > "$PORTFOLIO_ANALYZER_DOCKERFILE"
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \\
    git \\
    openjdk-17-jre-headless \\
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /$PORTFOLIO_ANALYZER_APPLICATION_PATH

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "run.py", "--server.port=8501", "--server.address=0.0.0.0"]
EOF

cat <<EOF > "$POSTGRES_INIT_SCRIPT_PATH"
-- Create databases
CREATE DATABASE ${POSTGRES_AIRFLOW_DATABASE};
CREATE DATABASE ${POSTGRES_PORTFOLIOANALYZER_DATABASE};

-- Create users and grant privileges
CREATE USER ${POSTGRES_AIRFLOW_USERNAME} WITH PASSWORD '${POSTGRES_AIRFLOW_PASSWORD}';
GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_AIRFLOW_DATABASE} TO ${POSTGRES_AIRFLOW_USERNAME};

CREATE USER ${POSTGRES_PORTFOLIOANALYZER_USERNAME} WITH PASSWORD '${POSTGRES_PORTFOLIOANALYZER_PASSWORD}';
GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_PORTFOLIOANALYZER_DATABASE} TO ${POSTGRES_PORTFOLIOANALYZER_USERNAME};

-- Ensure permissions on future tables and schemas
ALTER DATABASE ${POSTGRES_AIRFLOW_DATABASE} OWNER TO ${POSTGRES_AIRFLOW_USERNAME};
ALTER DATABASE ${POSTGRES_PORTFOLIOANALYZER_DATABASE} OWNER TO ${POSTGRES_PORTFOLIOANALYZER_USERNAME};

-- Grant privileges on schemas
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO ${POSTGRES_AIRFLOW_USERNAME};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO ${POSTGRES_PORTFOLIOANALYZER_USERNAME};
EOF

echo_success "Creating docker-compose.yaml"
cat <<EOF > "$DOCKER_COMPOSE_FILE_PATH"
version: '3.8'

services:
  airflow-scheduler:
    build:
      context: .
      dockerfile: $AIRFLOW_DOCKERFILE
    container_name: airflow-scheduler
    restart: always
    depends_on:
      - airflow-webserver
    environment:
      TZ: "Asia/Kolkata"
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__LOGGING__BASE_LOG_FOLDER: /opt/airflow/logs
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://$POSTGRES_AIRFLOW_USERNAME:$POSTGRES_AIRFLOW_PASSWORD@postgres/$POSTGRES_AIRFLOW_DATABASE
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://$POSTGRES_AIRFLOW_USERNAME:$POSTGRES_AIRFLOW_PASSWORD@postgres/$POSTGRES_AIRFLOW_DATABASE
      AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
      AIRFLOW__WEBSERVER__SECRET_KEY: "$AIRFLOW_WEBSERVER_SECRET_KEY"
      AIRFLOW_CONN_PORTFOLIOANALYZER_DB: "postgresql+psycopg2://${POSTGRES_PORTFOLIOANALYZER_USERNAME}:${POSTGRES_PORTFOLIOANALYZER_PASSWORD}@postgres/${POSTGRES_PORTFOLIOANALYZER_DATABASE}"
    networks:
      - $DOCKER_NETWORK_NAME
    volumes:
      - ./dags:/opt/airflow/dags
    entrypoint: ["airflow", "scheduler"]

  airflow-webserver:
    build:
      context: .
      dockerfile: $AIRFLOW_DOCKERFILE
    container_name: airflow-webserver
    restart: always
    depends_on:
      - postgres
      - redis
    environment:
      TZ: "Asia/Kolkata"
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__LOGGING__BASE_LOG_FOLDER: /opt/airflow/logs
      AIRFLOW__LOGGING__REMOTE_LOGGING: False
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://$POSTGRES_AIRFLOW_USERNAME:$POSTGRES_AIRFLOW_PASSWORD@postgres/$POSTGRES_AIRFLOW_DATABASE
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://$POSTGRES_AIRFLOW_USERNAME:$POSTGRES_AIRFLOW_PASSWORD@postgres/$POSTGRES_AIRFLOW_DATABASE
      AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
      AIRFLOW__WEBSERVER__SECRET_KEY: "$AIRFLOW_WEBSERVER_SECRET_KEY"
      AIRFLOW_CONN_PORTFOLIOANALYZER_DB: "postgresql+psycopg2://${POSTGRES_PORTFOLIOANALYZER_USERNAME}:${POSTGRES_PORTFOLIOANALYZER_PASSWORD}@postgres/${POSTGRES_PORTFOLIOANALYZER_DATABASE}"
    ports:
      - "8080:8080"
    networks:
      - $DOCKER_NETWORK_NAME
    volumes:
      - ./dags:/opt/airflow/dags
      - ./$AIRFLOW_ENTRYPOINT_SCRIPT_PATH:/entrypoint.sh
    entrypoint: ["/bin/bash", "/entrypoint.sh"]

  airflow-worker:
    build:
      context: .
      dockerfile: $AIRFLOW_DOCKERFILE
    container_name: airflow-worker
    restart: always
    depends_on:
      - airflow-scheduler
    environment:
      TZ: "Asia/Kolkata"
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__LOGGING__BASE_LOG_FOLDER: /opt/airflow/logs
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://$POSTGRES_AIRFLOW_USERNAME:$POSTGRES_AIRFLOW_PASSWORD@postgres/$POSTGRES_AIRFLOW_DATABASE
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://$POSTGRES_AIRFLOW_USERNAME:$POSTGRES_AIRFLOW_PASSWORD@postgres/$POSTGRES_AIRFLOW_DATABASE
      AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
      AIRFLOW__WEBSERVER__SECRET_KEY: "$AIRFLOW_WEBSERVER_SECRET_KEY"
      AIRFLOW__WEBSERVER__WEB_SERVER_HOST: "airflow-webserver"
      AIRFLOW_CONN_PORTFOLIOANALYZER_DB: "postgresql+psycopg2://${POSTGRES_PORTFOLIOANALYZER_USERNAME}:${POSTGRES_PORTFOLIOANALYZER_PASSWORD}@postgres/${POSTGRES_PORTFOLIOANALYZER_DATABASE}"
    networks:
      - $DOCKER_NETWORK_NAME
    volumes:
      - ./dags:/opt/airflow/dags
    entrypoint: ["airflow", "celery", "worker"]

  portfolio-analyzer:
    build:
      context: .
      dockerfile: $PORTFOLIO_ANALYZER_DOCKERFILE
    container_name: portfolio-analyzer
    restart: always
    depends_on:
      - airflow-webserver
      - postgres
    environment:
      - STREAMLIT_CONFIG_DIR=/$PORTFOLIO_ANALYZER_APPLICATION_PATH/.streamlit
    ports:
      - "8501:8501"
    networks:
      - $DOCKER_NETWORK_NAME

  postgres:
    image: postgres:latest
    container_name: postgres
    restart: always
    environment:
      POSTGRES_USER: $POSTGRES_USERNAME
      POSTGRES_PASSWORD: $POSTGRES_PASSWORD
    ports:
      - "5432:5432"
    volumes:
      - ./$POSTGRES_INIT_SCRIPT_PATH:/docker-entrypoint-initdb.d/postgres-init.sql
    networks:
      - $DOCKER_NETWORK_NAME

  redis:
    image: redis:latest
    container_name: redis
    restart: always
    ports:
      - "6379:6379"
    networks:
      - $DOCKER_NETWORK_NAME

networks:
  $DOCKER_NETWORK_NAME:
    driver: bridge
    ipam:
      config:
        - subnet: $DOCKER_NETWORK_SUBNET
EOF

echo_success "Building the Docker images"
docker compose -f "$DOCKER_COMPOSE_FILE_PATH" build

echo_success "Application successfully built!"
echo_success "Run \`docker compose up -d\` to deploy the application."
