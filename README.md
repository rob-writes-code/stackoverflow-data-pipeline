# Stack Overflow Data Pipeline

In this project, I created a data pipeline for Stack Overflow.

Please see below for an overview of the pipeline:

1. Data is extracted from a database held on the AWS RDS Cloud service.
2. Data is processed using Apache Airflow, running on an AWS EC2 instance.
3. Data is inserted into an analytical database, also held on AWS RDS.
4. The pipeline is scheduled to run every 15 minutes using Apache Airflow.

With regard to the dataset, it is an open-source archive of Stack Overflow content, including posts, votes, tags and badges.

In this project, I use a section of the dataset which amounts to approximately 55 million rows.

For a step-by-step walkthrough of the project, please see my Jupyter notebook, available here.

<hr>

Dataset is available on Kaggle: 
https://www.kaggle.com/datasets/stackoverflow/stackoverflow

A breakdown of the database schema can be accessed here:
https://sedeschema.github.io/
