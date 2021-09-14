# CompaniesRegistrationData_Analysis
## Project Description
This project aims to analyse Indian Companies Registration data by using Spark and SparkSQL .

## Technologies Used
- Hortonworks Data Platform - 2.6.5 (Minimum 12 gb RAM required)
- Hadoop - version 3.2.2
- Apache Spark - version 2.2.0
- PySpark - version 3.1.2

## Features
List of features :
- This project analyzes the companies registration data to get the
  - top 50 oldest companies.
  - companies which are vanished.
  - number of companies registered every year.
  - companies whose paidup capital is greater than authorized capital.
  - number of companies belonging to specific principal business. etc

To do :
- Adding more analyzation queries.
- Try it with RDD's

## Getting Started
1. Clone the project
```
$ git clone https://github.com/Mokshesh19/Project2.git
```
2. Install Oracle Virtual Box [here](https://www.virtualbox.org/wiki/Downloads).
3. Download Hortonworks virtual box ova file [here](https://www.cloudera.com/downloads/hdp.html).
4. Import the ova file in virtual box.

# Usage
1. Start the Virtual Machine.
2. After cloning the project if the dataset does not get downloaded, download it from [here](https://www.kaggle.com/rowhitswami/all-indian-companies-registration-data-1900-2019).
3. Open command prompt on your windows and give path to the directory where you have cloned the project and type the command given below.
```
$ scp -P 2222 ./registered_companies.csv.zip maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev/folder_name
```
4. Now as you have all files on you VM locally,unzip csv file and copy it on to HDFS
```
$ unzip registered_companies.csv.zip
$ hdfs dfs -put ./registered_companies.csv /user/maria_dev/folder_name
```
5. Then, you can run the .py file using sparksubmit.
```
$ spark-submit filename.py
```
