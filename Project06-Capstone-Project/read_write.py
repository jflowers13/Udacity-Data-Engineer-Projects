import os, glob
from pathlib import Path
from pyspark.sql.functions import *


#### Ingest Functions follow ####


def ingest_i94(spark, i94_data_path, staged_path, cache_flag=False):
    
    """
        Description:
            This function is used to ingest the I94 Immigration data
        
        Arguments:
            spark: Spark session object
            i94_data_path: Path to source of I94 data
            staged_path: Location to stage the data
            cache_flag: Flag to set Dataframe to be persistent
                        Defaults to False
                        Note: Useful for speeding up data checks
        
        Return:
            df_i94: Pyspark Dataframe with I94 data
    """
    
    # Encode path to I94 data source
    i94_pathlist = Path(i94_data_path).glob('i94_*_sub.sas7bdat')
    
    
    # Process each file into staging area
    for i94_file_path in i94_pathlist:
        
        # Get file path from path object
        i94_file = str(i94_file_path)
        
        # Ingest and write I94 file to staging area
        df_i94 = spark.read.format('com.github.saurfang.sas.spark').load(i94_file)
        print("Loading: {}".format(i94_file))
        df_i94.write.mode('append').parquet(os.path.join(staged_path, 'i94_data'))
        
    
    # Read combined data from staging area into pyspark dataframe
    df_i94 = spark.read.parquet(os.path.join(staged_path, 'i94_data'))
    
    # Set i94 dataframe to be persist if cache_flag is set
    if cache_flag == True:
        df_i94.cache()
    
    # Return pyspark dataframe
    return df_i94
    

def ingest_worldtemp(spark, worldtemp_path, cache_flag=False):
    
    """
        Description:
            This function is used to ingest World Temperature Data
        
        Arguments:
            spark: Spark session object
            worldtemp_path: Path to source of World Temperature Data
            cache_flag: Flag to set Dataframe to be persistent
                        Defaults to False
                        Note: Useful for speeding up data checks
        
        Return:
            df_worldtemp: World Temperature Dataframe
    """

    # Ingest World Temperature Data by City
    df_worldtemp = spark.read.csv(worldtemp_path, header=True, inferSchema=True)
    
    # Set worldtemp dataframe to be persist if cache_flag is set
    if cache_flag == True:
        df_worldtemp.cache()
    
    return df_worldtemp


def ingest_demog(spark, demog_path, cache_flag=False):
    
    """
        Description:
            This function is used to ingest US Cities Demographics Data
        
        Arguments:
            spark: Spark session object
            demog_path: Path to source of US Cities Demographics Data
            cache_flag: Flag to set Dataframe to be persistent
                        Defaults to False
                        Note: Useful for speeding up data checks
        
        Return:
            df_demog: US Cities Demographics Data Dataframe
    """
    
    # Ingest US Cities Demographics
    df_demog = spark.read.csv(demog_path, header=True, sep=';', inferSchema=True)
    
    # Set demog dataframe to be persist if cache_flag is set
    if cache_flag == True:
        df_demog.cache()
    
    return df_demog


def ingest_arptcodes(spark, arptcodes_path, cache_flag=False):
    
    """
        Description:
            This function is used to ingest Airport Codes
        
        Arguments:
            spark: Spark session object
            arptcodes_path: Path to source of Airport Codes Data
            cache_flag: Flag to set Dataframe to be persistent
                        Defaults to False
                        Note: Useful for speeding up data checks
        
        Return:
            df_arptcodes: Airport Codes Data Dataframe
    """
    
    # Ingest Airport Code Data by City
    df_arptcodes = spark.read.csv(arptcodes_path, header=True, inferSchema=True)
    
    # Set arptcodes dataframe to be persist if cache_flag is set
    if cache_flag == True:
        df_arptcodes.cache()
    
    return df_arptcodes



def read_lake(spark, output_path, fldr, cache_flag=False):
    
    """
        Description:
            This function reads parquet files in the data lake and returns a Spark dataframe
        
        Arguments:
            spark: Spark session object
            output_path: The path where the datalake was writen
            fldr: The table folder in the data lake
            cache_flag: Flag to set Dataframe to be persistent
                        Defaults to False
                        Note: Useful for speeding up data checks
        
        Return:
            df_spark: Spark dataframe
    """
    
    df_spark = spark.read.parquet(os.path.join(output_path, fldr))
    
    # Set arptcodes dataframe to be persist if cache_flag is set
    if cache_flag == True:
        df_spark.cache()
    
    return df_spark



#### Write Functions Follow ####


def write_i94_fact(df_i94, output_path, fldr):
    
    """
        Description:
            This function writes the I94 immigration fact table
        
        Arguments:
            df_i94: I94 immigration dataframe
            output_path: Path to data lake
            fldr: Folder for table in data lake
        
        Return:
            None
    """
    
    # Write Immigration Data Fact
    df_i94.select("cic_id", \
                  "i94_year", \
                  "i94_month", \
                  "birth_country_code", \
                  "res_country_code", \
                  "admit_port", \
                  "arrival_date", \
                  "trans_mode", \
                  "arr_state", \
                  "departure_date", \
                  "immigrant_age", \
                  "visa_code", \
                  "summary_stat", \
                  "file_add_date", \
                  "visa_post", \
                  "us_occupation", \
                  "arr_flag", \
                  "dep_flag", \
                  "upd_flag", \
                  "match_flag", \
                  "birth_year", \
                  "addmission_date", \
                  "gender", \
                  "ins_num", \
                  "airline_code", \
                  "admit_num", \
                  "flight_num", \
                  "visa_type") \
                  .write.mode("overwrite") \
                  .parquet(os.path.join(output_path, fldr))


def write_worldtemp_dim(df_worldtemp, output_path, fldr):
    
    """
        Description:
            This function writes the World Temperature dimension table
        
        Arguments:
            df_worldtemp: World Temperature dataframe
            output_path: Path to data lake
            fldr: Folder for table in data lake
        
        Return:
            None
    """
    
    # Write World Temperature by City Data Dimension
    df_worldtemp.select("date_stamp", \
                        "avg_temp", \
                        "avg_temp_uncert", \
                        "city", \
                        "country", \
                        "latitude", \
                        "longitude") \
                        .write.mode("overwrite") \
                        .parquet(os.path.join(output_path, fldr))


def write_demog_dim(df_demog, output_path, fldr):
    
    """
        Description:
            This function writes the US Demographics dimension table
        
        Arguments:
            df_demog: US Demographics dataframe
            output_path: Path to data lake
            fldr: Folder for table in data lake
        
        Return:
            None
    """
    
    # Write US City Demographics Dimension
    df_demog.select("city", \
                    "state", \
                    "median_age", \
                    "male_population", \
                    "female_population", \
                    "total_population", \
                    "num_of_veterans", \
                    "foreign_born", \
                    "avg_household_size", \
                    "state_code", \
                    "race", \
                    "count") \
                    .write.mode("overwrite") \
                    .parquet(os.path.join(output_path, fldr))



def write_arptcodes_dim(df_arptcodes, output_path, fldr):
    
    """
        Description:
            This function writes the Airport Codes dimension table
        
        Arguments:
            df_arptcodes: Airport Codes dataframe
            output_path: Path to data lake
            fldr: Folder for table in data lake
        
        Return:
            None
    """
    
    df_arptcodes.select("airport_id", \
                        "airport_type", \
                        "airport_name", \
                        "elevation_ft", \
                        "continent_abbrv", \
                        "country_code", \
                        "region_code", \
                        "municipality", \
                        "gps_code", \
                        "iata_code", \
                        "local_code", \
                        "latitude", \
                        "longitude") \
                        .write.mode("overwrite") \
                        .parquet(os.path.join(output_path, fldr))


