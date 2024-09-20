# *README TEAM 1: DATA ANALYST PROJECT*
## Work on a dataset of ABC Corporation employees. ##

<h1 align="center" id="title">Data Analysis Proyect</h1>

<p align="center">
  <img src="https://img.freepik.com/vector-premium/dispositivos-tecnologia-kawaii_24911-49452.jpg?w=740" alt="project-image">
</p>


<p id="description">Carrying out a complex data analysis process consisting of several phases, which will be explained below.</p>

## INDEX

- [Introduction](#Introduction)
- [Files](#Files)
- [Requirements](#Requirements)
- [The process](#the-process)
  - [Built with](#built-with)
  - [First phase](#First-phase-deep-data-exploration)
    - [Importations](#Importing-libraries-and-loading-data)
    - [General](#General-exploration-and-cleaning)
  - [Second phase](#Second-phase-Data-transformation)
  - [Third phase](#Third-phase-visualization)
  - [Fourth phase](#Fourth-phase-DataBase)
  - [Fifth phase](#Fifth-phase-ETL)
- [Author](#author)

## INTRODUCTION

Our mission is to identify key factors that influence job satisfaction and ultimately employee retention.
To this end, we have carried out a complex data analysis process including: EDA process, data transformation, A/B Testing, visualisations, creation of a MySQL database and ETL process.
  
## FILES

Files required for project review:

*    HR RAW EMPLOYEES.csv: contains information about ABC Corporation employees.
*    HR RAW DATA CLEAN.csv: CSV file created by us after a thorough cleaning of the data from the initial CSV.
*    BBDD_abc_corp_employees.sql: DB created by us from the CSV we generated after data cleansing.




## REQUIREMENTS

*Make sure you have the following libraries installed in your Python environment:*

1. pandas
2. numpy
3. matplotlib
4. seaborn
5. scikit-learn
6. mysql connector
7. scipy stats, chi2_contingency


If you do not have these libraries, you can install them using *pip install* 

## THE PROCESS 
### Built with

*Technologies used in the project:*

*   Operating system: Windows 10 Home
*   Development Environment: Jupyter Notebook, Visual Studio Code
*   Programming Language: Python
*   Libraries specified above
*   Version Control: Git, GitHub
*   Dependency Management: Pip
*   MySQL Workbench


### First phase: deep data exploration

#### Importing libraries and loading data:

*Use of pandas to load CSV files into DataFrames.*
<p align="center">
  <img src="imagenes/Importaciones.PNG" alt="project-image">
</p>


#### General exploration 

*   General deep review and analysis of data using Pandas functions to obtain information about the structure of the data and basic statistics.
*   Initial exploration of the data to identify potential problems (null values, duplicate values, outliers, missing data, etc.).
*   DataFrame joining

### Second phase: data transformation

*   Verification of data consistency and correctness.
*   Removing unnecesary columns
*   Homogenization of titles and values.
*   Treatment of negative numbers, outliers, null data and duplicated values.

### Third phase: visualization

Study of six real-world questions about the data and their representation through graphs.

### Fourth phase: DataBase

Creation of a DB (with clean DF) in MySQL Workbench, editing tables and their corresponding relations/restrictions. Lastly, creation of the DB diagram.

### Fifth phase: ETL

Data extraction, transformation and loading (ETL): -automation of the data insertion into the DB and the information transformation process to ensure that information is updated and inserted in a consistent manner.


## Author

Made with 💜 by [Belén V N (https://github.com/BelenVN), Gloria L C (https://github.com/GloriaLopezChinarro), Viviana V R (https://github.com/Viviana1988) y Cristina R H ]


# ENJOY IT 🤩
 
