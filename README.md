
# Change Data Capture (Postgres -> Debezium/Kafka -> Clickhouse -> Superset)

## Table of Contents

-  [Table of Contents](#table-of-contents)

-  [Introduction](#introduction)

-  [How to Start](#how-to-start)
	1. [Project Setup](#project-setup)
	2.  [OLTP Database: Postgres](#oltp-database-postgres)
	3.  [CDC: Debezium/Kafka](#cdc-with-debezium-and-kafka)
	4.  [OLAP Database: Clickhouse](#olap-database-clickhouse)


## Introduction

Rembo CDC is a data integration project that leverages Debezium, Kafka, and ClickHouse to stream data changes from a PostgreSQL database to a ClickHouse data warehouse. This project provides a streamlined approach for real-time data replication, enabling businesses to analyze and report on their PostgreSQL data in a highly efficient manner.


## How to Start

 ### Project Setup

Follow these steps to get started with  CDC:

1. Clone the Rembo CDC repository to your local machine using the following command:

```bash
git  clone  git@github.com:joelatiam/cdc.git
```

2. (Optional) Create/Start Python Virtual Environment:

- Create a virtual environment:
	```bash
	python  -m  venv  .venv
	```

- Activate the virtual environment:

	- On macOS and Linux:  

		```bash
		source  .venv/bin/activate
		```

3. Install dependencies:

	```bash
	pip  install  -r  requirements.txt
	```

### OLTP Database: Postgres

*Instructions on setting up and configuring the PostgreSQL database.*

For the OLTP component, [database replication should be logical](https://hevodata.com/learn/postgresql-logical-replication/#:~:text=To%20perform%20logical%20replication%20in,conf%20file.), and the  **db user** needs to have replication privileges **(ALTER ROLE db_user WITH REPLICATION LOGIN;)**. Follow these steps to set up the OLTP component:
  
1.  Create tables:
	- To [create/recreate](db/oltp/ddl/create_tables.py) the necessary tables, run the following command:
		```bash
		python  db/oltp/ddl/tables_creations.py
		```
		
2.  Alter tables with [Replica Identity](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY):
	```bash
	python  db/oltp/ddl/alter_tables.py
	```
3. Populate (Fake) Records to the rembo db

	- We are generating random records *(**500k:** customer, sales | **11** sales_territory | **5** employee)* to be used in our project

	```bash
	python  db/oltp/dml/create_random_records.py
	```

These steps will ensure that the PostgreSQL database is ready for logical replication and data capture.


### CDC with Debezium and Kafka

We will use **Debezium** to capture changes from our **Postgres**  and publish to **Kafka** topics.

 - Start applications (**zookeeper**, **kafka**, **debezium**, [**kafdrop**](https://github.com/obsidiandynamics/kafdrop): *view kafka topics, groups on the browser: http://localhost:9090/*)
	```bash
	cd cdc
	```
	```bash
	docker-compose up -d
	```
 - Create Debezium connector
	[This json.example](cdc/connectors/pg-src.json.example) **(cdc/connectors/pg-src.json.example)** is an example of the connector we will create by posting the [request](https://docs.confluent.io/platform/current/connect/references/restapi.html) to [debezium](https://debezium.io/documentation/reference/stable/tutorial.html) *(you can use **postman**, **vscode thunder client**, ...)* . Remember to update your dabase.info if you use **cdc/connectors/pg-src.json.example**
	
	eg: **POST** http://localhost:8083/connectors
	```bash
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{ "name": "inventory-connector", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1", "database.hostname": "mysql", "database.port": "3306", "database.user": "debezium", "database.password": "dbz", "database.server.id": "184054", "topic.prefix": "dbserver1", "database.include.list": "inventory", "schema.history.internal.kafka.bootstrap.servers": "kafka:9092", "schema.history.internal.kafka.topic": "schemahistory.inventory" } }' 
	```
	Debezium provides other [endpoints](https://docs.confluent.io/platform/current/connect/references/restapi.html) to helping to manage the connector
 - Update records and watch kafka messages

	 - Create/Update records from in our tables
	 - View/Read Kafka messages using **Kafkadrop** http://localhost:9090/
	 
		 **Message example**
		```bash
		{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"default":0,"field":"sales_id"},{"type":"int32","optional":true,"field":"customer_id"},{"type":"string","optional":true,"field":"discount_amount"}},"payload":{"sales_id":2, "customer_id":2496,"discount_amount":"85","duedate":"2023-10-19","extended_amount":843,"order_quantity":96,"product_standard_cost":22,"revision_number":62,"sales_amount":7,"sales_order_line_number":"2","sales_order_number":"2","sales_territory_id":3,"shipdate":19657,"tax_amt":55,"total_product_cost":77,"unit_price":21677,"employee_id":2,"__table":"sales","__op":"u","__lsn":12912566024,"__source_ts_ms":1698765730595,"__deleted":"false"}} 
		```
	

	 - (Optional) Listen to a kafka topic from the terminal.
		 We provided a python [script](cdc/kafka/watch-topics.py) (cdc/kafka/watch-topics.py) to watch a topic messages **(Remember to update the .env)**

		```bash
		python cdc/kafka/watch-topics.py 
		```

### OLAP Database: Clickhouse
