## Problem Statement:
Read a CSV file placed on the Google Cloud bucket, read it as part of a Composer DAG, load the data to Bigquery.

This can be further divided into these simple steps
- Learn about Composer
- Play with some sample Dags
- Place a CSV file in GCS bucket
- Write a DAG to read the csv file from the bucket and print the lines to console
- Create a Table in BQ to load the data into
- Extend the DAG to add another task, to load the csv data into BQ table

### About Composer:

- Cloud Composer is a managed Apache Airflow service that helps you create, schedule, monitor and manage workflows. Cloud Composer automation helps you create Airflow environments quickly and use Airflow-native tools, such as the powerful Airflow web interface and command line tools, so you can focus on your workflows and not your infrastructure

- Quick start: https://cloud.google.com/composer/docs/composer-2/run-apache-airflow-dag

### Creating a DAG:

- DAG stands for Directed Acyclic Graph. In simple terms, it is a graph with nodes, directed edges and no cycles.
- Airflow Operator: In an Airflow DAG, nodes are operators. In other words, a task in your DAG is an operator. An Operator is a class encapsulating the logic of what you want to achieve. For example, you want to execute a python function, you will use the PythonOperator. You want to execute a Bash command, you will use the BashOperator. Airflow brings a ton of operators that you can find here and here. When an operator is triggered, it becomes a task, and more specifically, a task instance.
- Dependencies: As you learned, a DAG has directed edges. Those directed edges are the dependencies in an Airflow DAG between all of your operators/tasks. Basically, if you want to say “Task A is executed before Task B”, you have to defined the corresponding dependency.
- Steps invloved:
1. Make the Imports
2. Create the Airflow DAG object
3. Add your tasks!
4. Defining dependencies
- Useful link: https://marclamberti.com/blog/airflow-dag-creating-your-first-dag-in-5-minutes/#Dependencies









