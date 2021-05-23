# You can freely edit this file based on your needs
export AIRFLOW_HOME=$(pwd)

#If you used Linux system, if not. Skip this steps
sudo apt-get install -y --no-install-recommends \
        freetds-bin \
        krb5-user \
        ldap-utils \
        libffi6 \
        libsasl2-2 \
        libsasl2-modules \
        libssl1.1 \
        locales  \
        lsb-release \
        sasl2-bin \
        sqlite3 \
        unixodbc

AIRFLOW_VERSION=2.0.2
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[async,postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install xlrd
pip install openpyxl

#Init DB based on AIRFLOW_HOME path
airflow db init

#Create account to login to Airflow web
AIRFLOW_USERNAME=admin
AIRFLOW_FIRSTNAME=Data
AIRFLOW_LASTNAME=Engineer
AIRFLOW_EMAIL=dataengineer@company.org

airflow users create \
    --username "${AIRFLOW_USERNAME}" \
    --firstname "${AIRFLOW_FIRSTNAME}" \
    --lastname "${AIRFLOW_LASTNAME}" \
    --role Admin \
    --email "${AIRFLOW_EMAIL}"

#Run on 1st terminal
airflow webserver --port 8080

#Run on separate terminal, but you have to export the AIRFLOW_HOME variable again before run this command
airflow scheduler

#Then open localhost:8080 on your browser and input your username and password that you just created