# pyspark-experimentation
Set up to enable me to develop using a local implementation of PySpark and to package the code up as a Python package.

## Pre-requisites
You need to have the following installed to run this project:
- Python
- Java
- Apache Spark
- winutils.exe

You also need to set up two environment variables:
- `SPARK_HOME` for example `set SPARK_HOME=C:\spark\spark-3.1.1-bin-hadoop2.7`
- `HADOOP_HOME` for example `set HADOOP_HOME=C:\spark\spark-3.1.1-bin-hadoop2.7\hadoop`

This approach was based on the following article:
https://towardsdatascience.com/installing-apache-pyspark-on-windows-10-f5f0c506bea1

**In addition**, you also need to set up third environment variable as follows:
`set PYSPARK_PYTHON=python`

## Running this project locally
Clone the repository locally.

At the command prompt, from the root of the project, initialise the Python virtual environment and install dependencies:
`pipenv install --dev`

To run unit tests:
`pipenv run pytest`

To run linting tests:
`pipenv run flake8`

To push package dependencies into setup.py file:
`pipenv run pipenv-setup sync`

To generate a requirements.txt file:
`pipenv lock -r > requirements.txt`

To generate package file:
`pipenv run python setup.py sdist bdist_wheel`