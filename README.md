
# Rembo CDC (Postgres to Clickhouse with Debezium/Kafka)

  

## Table of Contents

- [Rembo CDC (Postgres to Clickhouse with Debezium/Kafka)](#rembo-cdc-postgres-to-clickhouse-with-debeziumkafka)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [How to Start](#how-to-start)
      - [1. Clone the repository](#1-clone-the-repository)
  - [OLTP: Postgres](#oltp-postgres)

  

## Introduction

Rembo CDC is a data integration project that leverages Debezium, Kafka, and ClickHouse to stream data changes from a PostgreSQL database to a ClickHouse data warehouse. This project provides a streamlined approach for real-time data replication, enabling businesses to analyze and report on their PostgreSQL data in a highly efficient manner.

  

## How to Start

Follow these steps to get started with Rembo CDC:

  

#### 1. Clone the repository

1. Clone the Rembo CDC repository to your local machine using the following command:

	```bash
	git clone https://github.com/yourusername/rembo-cdc.git
	```

2. (Optional) Create/Start Python Virtual Environment:

	- Create a virtual environment:

		```bash
		python -m venv .venv
		```

	- Activate the virtual environment:

		- On Windows:

			```bash
			python -m venv .venv
			```

		- On macOS and Linux:

			```bash
			source .venv/bin/activate
			```

3. Install dependencies:

	```bash
	pip  install  -r  requirements.txt
	```

## OLTP: Postgres

*Instructions  on  setting  up  and  configuring  the  PostgreSQL  database.*

For  the  OLTP  component,  database  replication  should  be  logical,  and  the  user  should  have  replication  privileges.  Follow  these  steps  to  set  up  the  OLTP  component:


1.  **Create  tables:**

    -  To  create  the  necessary  tables,  run  the  following  command:

    	```bash
    	python db/oltp/ddl/tables_creations.py
    	```

2.  **Alter  tables  with  Replica  Identity:**

    -  To  alter  the  tables  with  Replica  Identity,  run  the  following  command:

    	```bash
    	python db/oltp/ddl/alter_tables.py
    	```

  

These  steps  will  ensure  that  the  PostgreSQL  database  is  ready  for  logical  replication  and  data  capture.
