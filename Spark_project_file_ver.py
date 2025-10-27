from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,concat,col,hash,isnull
import os
from datetime import datetime
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
# reading the Data already in output file
def read_tgt_data(lst):
    if len(lst)>=1:
        csv_files = [f for f in lst if f.endswith('.csv')]
        if csv_files:
            df1=spark.read.csv(os.path.join("F:\\Spark\\Project_1\\output_files",csv_files[0]),header=True,inferSchema=True).cache()
            return df1
    else:
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
# Function to load data to target
def main():
    #read data from input file into dataframe
    temp_df=0
    try:
        dt=datetime.now()
        year=dt.strftime("%Y")
        month=dt.strftime("%m")
        tst="F:\\Spark\\Project_1\\Spark_dataset\\yellow_tripdata_{}-{}.parquet".format(year,month)
        taxi_df=spark.read.parquet(tst)
        print(taxi_df.count()) #3475226
        #temp_df=taxi_df.limit(50)
        print(type(temp_df))
        temp_df=add_hash(temp_df)
    except Exception as e:
        print(e)
    try:
        lst=os.listdir("F:\\Spark\\Project_1\\output_files")
        print(len(lst))
        tgt_df=read_tgt_data(lst)
        tgt_df.show(2)
    except Exception as e:
        #print(e)
        pass
    if tgt_df is not None:
        #tgt_df.show(2)
        final_df=type_1_upd(tgt_df,temp_df)
        final_df.coalesce(1).write.csv("F:\\Spark\\Project_1\\output_files",header=True,mode="overwrite")
    else:
        try:
            temp_df.write.csv("F:\\Spark\\Project_1\\output_files",header=True,mode="overwrite")
            final_df=temp_df
            print("Here IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII AM")
        except Exception as e:
            print (e)
    return
main()