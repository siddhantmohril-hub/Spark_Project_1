from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,concat,col,hash,isnull,current_timestamp
from datetime import datetime
import cx_Oracle
import os
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars "F:\\JAVA\\JDBC_connection\\jars\\ojdbc11.jar" pyspark-shell'
# initialize Spark Session
spark = SparkSession.builder.appName("Spark_Project")\
        .config("spark.sql.streaming.fileStream.log.level", "ERROR")\
    .config("spark.sql.streaming.log.level", "ERROR")\
    .getOrCreate()
# adding a hash column
def add_hash(s_df):
    tdf=s_df.withColumn("key_hash",hash(concat(col("VendorID"),col("tpep_pickup_datetime"),col("tpep_dropoff_datetime"),\
                                         col("PULocationID"),col("DOLocationID"))))
    return tdf
# adding create timestamp to data
def add_timestamp(s_df):
    t_df=s_df.withColumn("CREATE_TS",current_timestamp())
    return t_df
# reading the Data already in output table
def read_tgt_data(cur):
    try:
        cur.execute("SELECT * FROM TAXI_TRIPDATA;")
        df1=spark.DataFrame(cur.fetchall())
        return df1
    except Exception as e:
        return None
# checking the update records with a type-1 logic
def type_1_upd(udf,tdf):
    cdf=udf.join(tdf,udf.key_hash==tdf.key_hash,"full_outer")
    cdf.select(udf.VendorID).show(5)
    final_df=cdf.select(udf.VendorID,udf.tpep_pickup_datetime,udf.tpep_dropoff_datetime,udf.passenger_count,udf.trip_distance,\
                    udf.RatecodeID,udf.store_and_fwd_flag,udf.PULocationID,udf.DOLocationID,udf.payment_type,udf.fare_amount,udf.extra,\
                        udf.mta_tax,udf.tip_amount,udf.tolls_amount,udf.improvement_surcharge,udf.total_amount,udf.congestion_surcharge,\
                            udf.Airport_fee,udf.cbd_congestion_fee,udf.key_hash)\
                                .filter(isnull(tdf.key_hash))
    final_df=final_df.union(cdf.select(tdf.VendorID,tdf.tpep_pickup_datetime,tdf.tpep_dropoff_datetime,tdf.passenger_count,tdf.trip_distance,\
                    tdf.RatecodeID,tdf.store_and_fwd_flag,tdf.PULocationID,tdf.DOLocationID,tdf.payment_type,tdf.fare_amount,tdf.extra,\
                        tdf.mta_tax,tdf.tip_amount,tdf.tolls_amount,tdf.improvement_surcharge,tdf.total_amount,tdf.congestion_surcharge,\
                            tdf.Airport_fee,tdf.cbd_congestion_fee,tdf.key_hash)\
                                .filter(isnull(udf.key_hash)))
    final_df=final_df.union(cdf.select(udf.VendorID,udf.tpep_pickup_datetime,udf.tpep_dropoff_datetime,udf.passenger_count,udf.trip_distance,\
                    udf.RatecodeID,udf.store_and_fwd_flag,udf.PULocationID,udf.DOLocationID,udf.payment_type,udf.fare_amount,udf.extra,\
                        udf.mta_tax,udf.tip_amount,udf.tolls_amount,udf.improvement_surcharge,udf.total_amount,udf.congestion_surcharge,\
                            udf.Airport_fee,udf.cbd_congestion_fee,udf.key_hash)\
                                .filter((~isnull(udf.key_hash)) & (~isnull(tdf.key_hash))))
    
    #final_df.show()                                        
    return final_df
def dbconnect():
    #cx_Oracle.init_oracle_client(lib_dir=r"C:\instantclient_21_7")
    hostname='localhost'
    username='system'
    password='Oct_2k25'
    SID='oracldb'
    try:
        connection=cx_Oracle.connect(username,password,'{0}/{1}'.format(hostname,SID))
        print('Connection successful')
        cur=connection.cursor()
        return cur
    except Exception as e:
        return e
# Function to load data to target
def dataload_tgt(cur,df):
# Query to create table for the first time load
    q='CREATE TABLE \
        TAXI_TRIPDATA (\
            VendorID NUMBER(10),\
                tpep_pickup_datetime TIMESTAMP,\
                tpep_dropoff_datetime TIMESTAMP,\
                passenger_count NUMBER(10),\
                trip_distance NUMBER(10,2),\
                RatecodeID NUMBER(10,2),\
                store_and_fwd_flag   CHAR(1),\
                PULocationID         NUMBER(10),\
                DOLocationID         NUMBER(10),\
                payment_type         NUMBER(10),\
                fare_amount          NUMBER(10,2),\
                extra                NUMBER(10,2),\
                mta_tax              NUMBER(10,2),\
                tip_amount           NUMBER(10,2),\
                tolls_amount         NUMBER(10,2),\
                improvement_surcharge   NUMBER(10,2),\
                total_amount         NUMBER(10,2),\
                congestion_surcharge NUMBER(10,2),\
                Airport_fee          NUMBER(10,2),\
                cbd_congestion_fee   NUMBER(10,2),\
                key_hash VARCHAR2(256),\
                CREATE_TS TIMESTAMP);'
    cur.execute(q)
#truncating the existing data
    #cur.execute('TRUNCATE TABLE TAXI_TRIPDATA;')
    cur.execute('COMMIT')
# load the target data from dataframe to database
    oracle_properties={
        "driver":"oracle.jdbc.driver.OracleDriver",
        "url": "jdbc:oracle:thin:@localhost:1521:oracldb",
        "user": "system",
        "password": "Oct_2k25"
    }
    df.write.format("jdbc") \
    .option("url", oracle_properties["url"]) \
    .option("driver", oracle_properties["driver"]) \
    .option("dbtable", "TAXI_TRIPDATA") \
    .option("user", oracle_properties["user"]) \
    .option("password", oracle_properties["password"]) \
    .mode("overwrite").save()
def main():
    #read data from input file into dataframe
    try:
        dt=datetime.now()
        year=dt.strftime("%Y")
        month=dt.strftime("%m")
        tst="F:\\Spark\\Project_1\\Spark_dataset\\yellow_tripdata_{}-{}.parquet".format(year,month)
        taxi_df=spark.read.parquet(tst)
        #print(taxi_df.count()) #3475226
        temp_df=taxi_df.limit(50)
    except Exception as e:
        if ('FileNotFoundException' in str(e)):
            print('No Data Available for the month')
    cursor=dbconnect()
    temp_df=add_hash(temp_df)
    temp_df=add_timestamp(temp_df)
    #print(type(cursor))
    tgt_df=read_tgt_data(cursor)
    if tgt_df is not None:
        #tgt_df.show(2)
        final_df=type_1_upd(tgt_df,temp_df)
    else:
        final_df=temp_df
    dataload_tgt(cursor,final_df)
    return
main()

#Problems
#1 Throws error when file not available for the month (Try-except not handling the issue)
#2 create table if exists doesnt work for oracle 19c. Need a workaround
#3 create table query is not working using cx_oracle
#4 ojdbc11.jar file cannot be located from path variable