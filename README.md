# File Ingestion Process
## Overview
The File Ingestion Process is a Python-based data pipeline designed to ingest JSON files into a PostgreSQL database. This README provides an overview of the key components and functionality of the pipeline.

# Table of Contents
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Deployment](#deployment)
- [License](#license)
- [Contributing](#contributing)  
- [Authors & Acknoledgements](#authors_and_acknowledgments)

## Introduction <a name="introduction"></a>
The File Ingestion Process consists of Python scripts that facilitate the ingestion of JSON files into a PostgreSQL database. It leverages the psycopg2 and sqlalchemy libraries for database connectivity and the pandas library for data manipulation.

## Installation <a name="installation"></a>
#### Prerequisites <a name="prerequisites"></a>
Before running the File Ingestion Process, ensure you have the following prerequisites installed:
- Python 3.x
- PostgreSQL database

## Configuration <a name="configuration"></a>
Create database_credentials file and fill in the info
```
nano /home/server_user/database_credentials/config.ini
[database]
ods_host=localhost
ods_port=5432
ods_username=database_username
ods_password=database_password
ods_database_name=database_name

stg_host=localhost
stg_port=5432
stg_username=database_username
stg_password=database_password
stg_database_name=database_name
```

## Usage <a name="usage"></a>
Navigate to server_user folder and create virtual environment
```
cd /home/server_user
python3 -m venv server_user_venv
```

Clone the repository to your local machine:
``` 
git clone https://github.com/Data-Fi-Nigeria-server_user/server_user_sync_ingestion.git
```

Navigate to the ingestion pipeline directory:
``` 
cd server_user_sync_ingestion
```

Install the required Python packages:
```
pip install -r requirements.txt
```

Run Ingestion Pipeline Manually to test
```
nohup python file_ingestion_process.py &
nohup python dsd_ingestion_process.py &
```

## Deployment <a name="deployment"></a>
Automate bash scripts to run periodically
```
crontab -e
0 * * * * /home/server_user/server_user_ingestion_pipeline/orchestrate_file_ingestion.sh
*/30 * * * * /home/server_user/server_user_ingestion_pipeline/orchestrate_dsd_file_ingestion.sh
0 0 * * 0 /home/server_user/server_user_ingestion_pipeline/orchestrate_file_delete.sh
0 */1 * * * /home/server_user/server_user_ingestion_pipeline/orchestrate_stg_record_delete.sh
```

## License <a name="license"></a>
- MIT License

## Authors & Acknowledgements <a name="authors_and_acknowledgments"></a>
- Uche 



