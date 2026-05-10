#!/usr/bin/env bash
set -euo pipefail

ENV_FILE=".env"

echo "=== Initializing project ==="

# Ensure script runs from repo root
cd "$(dirname "$0")"

APT_UPDATED=0

install_apt_package() {
  local package="$1"
  local reason="$2"

  if ! command -v apt-get >/dev/null 2>&1; then
    echo "ERROR: $reason"
    echo "Install package manually because apt-get is not available: $package"
    exit 1
  fi

  if ! command -v sudo >/dev/null 2>&1; then
    echo "ERROR: $reason"
    echo "Install package manually because sudo is not available: $package"
    exit 1
  fi

  echo "Installing missing dependency: $package"
  if [ "$APT_UPDATED" -eq 0 ]; then
    sudo apt-get update
    APT_UPDATED=1
  fi
  sudo apt-get install -y "$package"
}

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: Docker CLI is not installed or not available in this WSL shell"
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "ERROR: Docker Compose is not available through 'docker compose'"
  exit 1
fi

if ! command -v unzip >/dev/null 2>&1; then
  install_apt_package "unzip" "unzip is required to extract Kaggle datasets"
fi

if command -v id >/dev/null 2>&1; then
  AIRFLOW_UID_VALUE="$(id -u)"
else
  AIRFLOW_UID_VALUE=50000
  echo "WARN: Cannot detect current user ID. Falling back to AIRFLOW_UID=$AIRFLOW_UID_VALUE"
fi

# Kaggle config
if [ -f ".kaggle/kaggle.json" ]; then
  mkdir -p "$HOME/.kaggle"
  cp ".kaggle/kaggle.json" "$HOME/.kaggle/kaggle.json"
  chmod 600 "$HOME/.kaggle/kaggle.json" || true
  export KAGGLE_CONFIG_DIR="$HOME/.kaggle"
elif [ -f "$HOME/.kaggle/kaggle.json" ]; then
  chmod 600 "$HOME/.kaggle/kaggle.json" || true
  export KAGGLE_CONFIG_DIR="$HOME/.kaggle"
else
  echo "ERROR: Cannot find kaggle.json"
  echo "Place it at either:"
  echo "  ./.kaggle/kaggle.json"
  echo "  ~/.kaggle/kaggle.json"
  exit 1
fi

echo "Using Kaggle config: $KAGGLE_CONFIG_DIR"

# Clean old generated data
rm -f hiveconf/hiveserver2.pid || true
rm -rf spark/data/checkpoint || true

mkdir -p nifi/data
mkdir -p postgresDB/backup
mkdir -p airflow/dags airflow/logs airflow/plugins airflow/config
mkdir -p packages

# Write fresh .env
cat > "$ENV_FILE" <<EOF
AIRFLOW_UID=$AIRFLOW_UID_VALUE
AIRFLOW_PROJ_DIR=./airflow
_AIRFLOW_WWW_USER_USERNAME=thanhdn
_AIRFLOW_WWW_USER_PASSWORD=thanhdn
AIRFLOW_IMAGE_NAME=nghia294/ariflow-pro:v1.0
POSTGRES_LOCAL_PATH=packages/postgresql.jar
ELASTIC_PASSWORD=thanhdn
EOF

echo ".env created"

echo "=== Initializing Airflow with Docker ==="
docker compose up airflow-init

# Install Kaggle CLI in a project-local virtualenv. This avoids relying on
# system pip, which is often absent in fresh Ubuntu WSL installs.
if command -v python3 >/dev/null 2>&1; then
  PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_CMD="python"
else
  echo "ERROR: Python is not installed"
  exit 1
fi

KAGGLE_VENV=".venv"

if ! "$PYTHON_CMD" -m venv --help >/dev/null 2>&1; then
  if [ "$PYTHON_CMD" = "python3" ]; then
    install_apt_package "python3-venv" "Python venv support is required to install Kaggle CLI"
  else
    echo "ERROR: Python venv support is required to install Kaggle CLI"
    exit 1
  fi
fi

if [ ! -x "$KAGGLE_VENV/bin/python" ]; then
  "$PYTHON_CMD" -m venv --copies "$KAGGLE_VENV"
fi

"$KAGGLE_VENV/bin/python" -m pip install -q --upgrade pip
"$KAGGLE_VENV/bin/python" -m pip install -q kaggle

KAGGLE_BIN="$PWD/$KAGGLE_VENV/bin/kaggle"

if [ ! -x "$KAGGLE_BIN" ]; then
  echo "ERROR: Kaggle CLI not found after install"
  exit 1
fi

download_dataset() {
  DATASET="$1"
  ZIP_NAME="$2"
  EXTRACT_DIR="$3"
  TARGET_DIR="$4"

  echo "Downloading dataset: $DATASET"

  rm -rf "$ZIP_NAME" "$EXTRACT_DIR"
  "$KAGGLE_BIN" datasets download -d "$DATASET"

  echo "Extracting $ZIP_NAME"
  unzip -o "$ZIP_NAME" -d "$EXTRACT_DIR"

  mkdir -p "$TARGET_DIR"
  mv "$EXTRACT_DIR"/* "$TARGET_DIR"/

  rm -rf "$ZIP_NAME" "$EXTRACT_DIR"
}

download_dataset \
  "ren294/ecommerce-clickstream-transactions" \
  "ecommerce-clickstream-transactions.zip" \
  "ecommerce-clickstream-transactions" \
  "nifi/data"

download_dataset \
  "ren294/access-log-ecommerce" \
  "access-log-ecommerce.zip" \
  "access-log-ecommerce" \
  "nifi/data"

download_dataset \
  "ren294/ecom-postgres" \
  "ecom-postgres.zip" \
  "ecom-postgres" \
  "postgresDB/backup"

echo "=== Initialization completed successfully ==="
echo "Next step:"
echo "  docker compose --profile all up -d"
