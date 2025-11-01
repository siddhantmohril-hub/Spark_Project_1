# Spark_Project_1
My first project for doing an SCD type 1 update of a taxi data received monthly in a parquet file and loading into oracle database as an Incremantal load

*Spark_project_1.py*: The pySpark code for reading data from the source parquet files and loading to the oracle database

*Spark_project_file_ver*: Older pySpark code for reading data from source parquect file and creating an output file with updated data

*Database*: Database is created in an Oracle 19c database. Did the create query using cx_Oracle class but loaded the data from Spark dataframe using Spark's jdbc driver as cx_oracle does not support writing data from a Spark RDD

**The project is currently not finished. I am facing the below problems with it:**

- Throws error when file not available for the month (Try-except not handling the issue) - not solved. Just dont run without file
- Create table if exists doesnt work for oracle 19c. Need a workaround - created table manually 
- Create table query is not working using cx_oracle  - created table manually 
- ojdbc11.jar file cannot be located from path variable - added the variable in the code

**[Edit] Status of the above problems:**

- Throws error when file not available for the month (Try-except not handling the issue)-
    Solved: adjusted the configuration to logging level as "Error" by adding .config("spark.sql.streaming.log.level","Error")
    and added config.("spark.driver.extraJavaOptions","-Dlog4j.rootCategory=Error") for spark driver warnings
- create table if not exists doesnt work for oracle 19c- 
    Solved: added a logic to check all_tables for presence of the given table
- create table query is not working using cx_oracle-
    Solved: Create table query works now, removed ';' at the end of the query
- ojdbc11.jar file cannot be located from path variable - added the variable in the code-
    Not Solved: gives warning about not being able to delete the ojjdbc.jar file after code run

Please feel free to provide suggestions/rants on the code.