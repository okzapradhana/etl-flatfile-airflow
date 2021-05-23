# ETL with Airflow, Google Cloud Storage and BigQuery
ETL from flat file data sources to Data Warehouse.

## **Data Sources**
1. SQLite
2. Comma separated values* (CSV)
3. Excel (xls, xlsx)
4. JSON

## **Tech Stacks**
1. Python (v3.8.5)
2. Airflow (v2.0.2)
3. Google Cloud Storage (GCS)
4. BigQuery

## **Installation Setup**
### Setup Airflow
#### - **Native Airflow**
This repo is using Native Airflow that is intended to get understanding on how to setup Airflow from scratch and for the sake of learning. Here is the steps to setup:
1. **(Highly recommended)** Create virtual environment and activate it by running
   ```bash
   python -m venv venv 
   source ./venv/bin/activate
   ```
2. Install `apache-airflow` with some libraries contraints that compatible with `AIRFLOW_VERSION` and `PYTHON_VERSION` to prevent any system break because of incompatibility
   ```bash
   AIRFLOW_VERSION=2.0.2
   PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
   CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
   pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
   ```
3. Run `export AIRFLOW_HOME=$(pwd)` to set `AIRFLOW_HOME` variable to your current directory, but this is optional. The default value is `AIRFLOW_HOME=~/airflow` 
4. Run `airflow db init` to initialize SQLite Database which stores Airflow metadata based on your `AIRFLOW_HOME` variable
5. Create user account by running
   ```bash
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
   ```
   You will need to input your password when executing this command. And its your freedom to change the `USERNAME`, `FIRSTNAME` , `LASTNAME`, and `EMAIL` variable based your needs.
6. On same terminal, start the Airflow webserver with: 
   ```bash
   airflow webserver --port 8080
   ```
7. Open new terminal, run the scheduler to make your `dags` can do their tasks. Notice that you have to set the `AIRFLOW_HOME` variable again if you have set the variable before:
   ```bash
   export AIRFLOW_HOME=$(pwd)
   airflow scheduler
   ``` 
8. Voila! Just open `http://localhost:8080/` on your browser to see the Airflow web

Shortly, you may run `install.sh` to perform the installation. Again, you can edit `install.sh` based on needs.
More on: https://airflow.apache.org/docs/apache-airflow/stable/start/local.html

#### - **Alternative: Use Airflow Docker**
But if you wish to use the Docker Airflow instead you can lookup to `docker-compose.yaml` file and refer to this tutorial: https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html . 

If you need to install other dependecies other than what have been installed by the airflow docker image, please refer to this page: https://airflow.apache.org/docs/apache-airflow/1.10.13/production-deployment.html#extending-the-image

### BigQuery Setup
This project is used some of GCP services which is GCS and BigQuery, and we need to do some stuff on them.
1. On your GCP console, go to BigQuery. You can find it on `Big Data > BigQuery`
2. Then create your Dataset ![create-dataset](/images/Create%20Dataset%20menu.png)
3. Fill the Dataset field such as. In this project we only need to set:
   - **Data set ID** (example: my_first_bigquery)
   - **Data location**. Choose the nearest one from your location.
  ![fill-dataset](/images/Create%20BigQuery%20Dataset.png)
   
4. Click `CREATE DATA SET`
5. Ensure that your dataset has been created
![ensure-dataset-created](/images/Ensure%20BQ%20Dataset%20created.png)
<br><br>
### Google Cloud Storage Setup
1. Back to your GCP console, choose Cloud Storage. You can find it on `Storage > Cloud Storage`
2. Click `CREATE BUCKET` button. Then fill some fields such as:
   - Name your bucket (example: blank-space-de-batch1)
   - Choose where to store your data
     - I would suggest to choose **Region** option because it offers lowest latency and single region. But if you want to get high availability you may consider to choose other location type options.
     - Then choose nearest location from you
   - Leave default for the rest of fields.
3. Click `CREATE`
4. Your bucket will be created and showed on GCS Browser
   ![success-create-bucket](images/Success%20Create%20Bucket.png)
<br><br>
### Service Account
To recognized by Google Cloud Platform and interact with them. We have to set our service account.
1. Go to GCP Console again, then navigate to `Products > IAM & Admin > Service Accounts`
2. Click `CREATE SERVICE ACCOUNT` button
3. Fill Service account details by filling `Service account name` and `Service account description` then click `CREATE` 
   <br>Example:
   ![fill-service-account](images/Fill%20Create%20Service%20Account.png)
4. Click `DONE`
5. Your service account can be seen on the main page
   ![service-account](/images/Service%20accounts.png)
6. As you can see this service account doesn't have key yet. Thus we need to create first. On **Action** column click the icon then choose **Manage keys**
7. On Keys page, click `ADD KEY` and choose **Create new key**.
8. Choose `JSON` as the key type then click `CREATE`
9. The key will be automatically created & downloaded to your computer.
  ![created-key](/images/Key%20created%20and%20downloaded%20to%20computer.png)

**Notes**

This `json` key will be needed when adding/editing Airflow Connections later. Thus please keep it safe.

## Airflow Connections
To connect our Airflow with external system, we need to setup connections on Airflow.
1. On http://localhost:8080, go to **Admin > Connections**
2. Add or Edit current Connection. Search for Google Cloud conn type
3. Input fields needed there:
   1. **Conn Id** (example: my_google_cloud_conn_id)
   2. **Conn Type**: Google Cloud
   3. **Description** (example: To interact with Google Cloud Platform such as upload data to GCS, load data from GCS to BigQuery, etc. )
   4. **Keyfile Path**. This path is where your service account key is located. Refer to that path and fill this field with those file path.
   5. **Keyfile JSON**. If you use Keyfile Path, leave this blank
   6. **Number of Retries**. Default value is 5, but I set to 2.
   7. **Project Id**. Set this value to your GCP Project Id.
   8. **Scopes (comma separated)**. People on forum recommends to fill this with https://www.googleapis.com/auth/cloud-platform
   9. Click `Save` button
   10. Done! Everytime your Airflow connector needs `GCP conn_id`, just fill it with your **Conn Id**

Example:
![connections-airflow](images/Setup%20GCP%20Connections%20in%20Airflow.png)

## Airflow Variables
Airflow Variables is very important if you want to set global value which can accessed to your DAGs. Here's how to do it:
1. On http://localhost:8080 go to **Admin > Variables**
2. Click the **Plus (+)** icon. Or you can just Import Variables which is json file containing key value of variables.
3. At very least, this projects must have this Variables:
   1. BASE_PATH
   2. BUCKET_NAME
   3. DATASET_ID
   4. GOOGLE_CLOUD_CONN_ID
4. Done! Now your DAGs can access this variable by using:
   ```python
   from airflow.models.variable import Variable
   
   DATASET_ID = Variable.get("DATASET_ID")
   ...
   ```

Example:
![variables-airflow](images/Set%20Airflow%20Variable.png)

## How to Use
Everytime you want to run Airflow on your computer. Do this: 
1. Activate your virtual environment by executing `source venv/bin/activate`
2. Run `airflow webserver --port 8080 ` at your current terminal
3. Run `airflow scheduler` on your other terminal.
4. Go to http://localhost:8080 