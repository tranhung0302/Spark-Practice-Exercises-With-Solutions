# Spark Practice Exercises with Solutions

Welcome! This repository contains a collection of hands-on Spark exercises inspired by Jacek [Laskowski's Spark workshop](https://jaceklaskowski.github.io/spark-workshop). The goal is to help you practice and master Apache Spark using practical examples and solutions.

## Spark Environment Setup

These exercises use Apache Spark running locally with the PySpark API (Python). You are free to use any Spark setup you prefer (e.g., Databricks, EMR, or Spark with Scala/Java), but instructions and code samples will focus on PySpark for simplicity.

To use PySpark, you need a Python environment and Java installed on your machine.

- Install Python with conda (recommended):
  - Create a new Python enviroment
    ```bash
    conda create -n spark-env python=3.10
    conda activate spark-env
    ```
  - Install PySpark:
    ```bash
    pip install pyspark==4.0.0
    ```
  - Verify your installation:
    ```bash
    pyspark --version
    ```

- Install Java (required for Spark 4):
  - On macOS, you can use Homebrew:
    ```bash
    brew install openjdk@17
    ```
  - Set JAVA_HOME for Py4J to find the JVM
    ```bash
    export JAVA_HOME="/usr/local/opt/openjdk@17"
    ```

- Check if your Python can connect to JAVA
  - Back to Python and run:
    ```
    pyspark
    ```

## Contributing
Contributions are welcome! Feel free to submit pull requests with new exercises, solutions, or improvements.

---
Happy Sparking!


