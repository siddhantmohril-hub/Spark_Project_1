# Spark_Project_1
My first project for doing an SCD type 1 update of a taxi data received monthly in a parquet file and loading into oracle database

The project is currently not finished. I am facing the below problems with it:
1 Throws error when file not available for the month (Try-except not handling the issue) - not solved. Just dont run without file
2 create table if exists doesnt work for oracle 19c. Need a workaround - created table manually 
3 create table query is not working using cx_oracle  - created table manually 
4 ojdbc11.jar file cannot be located from path variable - added the variable in the code
