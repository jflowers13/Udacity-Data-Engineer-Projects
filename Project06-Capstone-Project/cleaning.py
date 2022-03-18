from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import types as T
from pyspark.sql.types import StructType as R, StructField as Fld
import pyspark.sql.functions



def clean_i94(df_i94):
    
    """
        Description:
            Cleans the I94 Immigration data by renaming columns, adjusting data types,
            converting from SAS date to date and removing duplicate rows.
        Arguments:
            df_i94: Dataframe containing I94 Immigration data
        
        Return:
            df_i94: Dataframe containing clean I94 Immigration data
    """
    
    # Clean I94 Immigration data

    # Make column names readable and correct data types
    df_i94 = df_i94.withColumn("cic_id", col("cicid").cast("integer")).drop("cicid") \
                            .withColumn("i94_year", col("i94yr").cast("integer")).drop("i94yr") \
                            .withColumn("i94_month", col("i94mon").cast("integer")).drop("i94mon") \
                            .withColumn("birth_country_code", col("i94cit").cast("integer")).drop("i94cit") \
                            .withColumn("res_country_code", col("i94res").cast("integer")).drop("i94res") \
                            .withColumn("admit_port", col("i94port")).drop("i94port") \
                            .withColumn("trans_mode", col("i94mode").cast("integer")).drop("i94mode") \
                            .withColumn("arr_state", col("i94addr")).drop("i94addr") \
                            .withColumn("immigrant_age", col("i94bir").cast("integer")).drop("i94bir") \
                            .withColumn("visa_code", col("i94visa").cast("integer")).drop("i94visa") \
                            .withColumn("summary_stat", col("count").cast("integer")).drop("count") \
                            .withColumn("visa_post", col("visapost").cast("integer")).drop("visapost") \
                            .withColumn("us_occupation", col("occup")).drop("occup") \
                            .withColumn("arr_flag", col("entdepa")).drop("entdepa") \
                            .withColumn("dep_flag", col("entdepd")).drop("entdepd") \
                            .withColumn("upd_flag", col("entdepu")).drop("entdepu") \
                            .withColumn("match_flag", col("matflag")).drop("matflag") \
                            .withColumn("birth_year", col("biryear").cast("integer")).drop("biryear") \
                            .withColumn("ins_num", col("insnum")).drop("insnum") \
                            .withColumn("airline_code", col("airline")).drop("airline") \
                            .withColumn("admit_num", col("admnum").cast("integer")).drop("admnum") \
                            .withColumn("flight_num", col("fltno")).drop("fltno") \
                            .withColumn("visa_type", col("visatype")).drop("visatype") \
                            .withColumn("arr_date_int", col("arrdate").cast("integer")).drop("arrdate") \
                            .withColumn("dep_date_int", col("depdate").cast("integer")).drop("depdate") 

    # Define function for converting data from SAS to date
    def convert_dt(x):
        try:
            start = datetime(1960, 1, 1)
            return start + timedelta(days=int(x))
        except:
            return None

    # Define UDF for converting SAS to date
    get_date = udf(lambda x: convert_dt(x), T.DateType())

    # Convert SAS dates to date
    df_i94 = df_i94.withColumn('arrival_date', get_date(df_i94.arr_date_int)) \
                                    .drop("arr_date_int") \
                                    .withColumn("departure_date", get_date(df_i94.dep_date_int)) \
                                    .drop("dep_date_int")

    # Convert string date to date
    df_i94 = df_i94.withColumn("file_add_date", to_date(col("dtadfile"), "yyyyMMdd")) \
                                    .drop("dtadfile") \
                                    .withColumn("addmission_date", to_date(col("dtaddto"), "MMddyyyy")) \
                                    .drop("dtaddto")

    # Remove duplicate rows
    df_i94 = df_i94.dropDuplicates()
    
    return df_i94


def clean_demog(df_demog):
    
    """
        Description:
            Cleans the US Demographics data by renaming columns, changing data types,
            replacing nulls in numerical columns with 0 and removing duplicate rows.
            
        Arguments:
            df_demog: Dataframe containing US Demographics data
        
        Return:
            df_demog: Dataframe containing cleaned US Demographics data
    """
        
    # Columns renamed to make lower case and remove characters not valid for the parquet format
    df_demog = df_demog.withColumnRenamed("City", "city") \
                            .withColumnRenamed("State", "state") \
                            .withColumnRenamed("Median Age", "median_age") \
                            .withColumnRenamed("Male Population", "male_population") \
                            .withColumnRenamed("Female Population", "female_population") \
                            .withColumnRenamed("Total Population", "total_population") \
                            .withColumnRenamed("Number of Veterans", "num_of_veterans") \
                            .withColumnRenamed("Foreign-born", "foreign_born") \
                            .withColumnRenamed("Average Household Size", "avg_household_size") \
                            .withColumnRenamed("State Code", "state_code") \
                            .withColumnRenamed("Race", "race") \
                            .withColumnRenamed("Count", "count")

    # Fill null numerical columns with 0
    df_demog = df_demog.fillna({'median_age':0, \
                                'male_population':0, \
                                'female_population':0, \
                                'total_population':0, \
                                'num_of_veterans':0, \
                                'avg_household_size':0, \
                                'count':0})

    # Drop duplicate records
    df_demog = df_demog.dropDuplicates()
    
    return df_demog


def clean_worldtemp(df_worldtemp):
    
    """
        Description:
            Cleans the World Temperature data by renaming columns, adjusting data types,
            removing rows with null average temperature and removing duplicate rows.
            
        Arguments:
            df_worldtemp: Dataframe containing World Temperatures
        
        Return:
            df_worldtemp: Dataframe containing cleaned World Temperatures
    """    
    
    # Clean Global Temperature Data

    # Rename columns and adjust data types
    df_worldtemp = df_worldtemp.withColumn("date_stamp", col('dt').cast("date")) \
                                        .drop("dt") \
                                        .withColumn("avg_temp", col("AverageTemperature").cast("float")) \
                                        .drop("AverageTemperature") \
                                        .withColumn("avg_temp_uncert", col("AverageTemperatureUncertainty").cast("float")) \
                                        .drop("AverageTemperatureUncertainty") \
                                        .withColumnRenamed("City", "city") \
                                        .withColumnRenamed("Country", "country") \
                                        .withColumnRenamed("Latitude", "latitude") \
                                        .withColumnRenamed("Longitude", "longitude")

    # Remove rows with null average temperature
    df_worldtemp = df_worldtemp.dropna(subset=['avg_temp'])

    # Remove duplicate rows
    df_worldtemp = df_worldtemp.dropDuplicates()
    
    return df_worldtemp


def clean_arptcodes(df_arptcodes):
    
    """
        Description:
            Cleans the Airport Codes data by splitting the malformed coordinate field into
            latitude and longitude and renaming columns.
            
        Arguments:
            df_arptcodes: Dataframe containing Airport Codes
        
        Return:
            df_arptcodes: Dataframe containing cleaned Airport Codes
    """    
    
    # Split coordinate field into latitude and longitude
    # Note, coordinates in the data source are backwards as confirmed
    # with Google Maps and is cleaned and split here
    df_arptcodes = df_arptcodes \
                        .withColumn("latitude", split(col("coordinates"), ", ").getItem(1).cast("double")) \
                        .withColumn("longitude", split(col("coordinates"), ", ").getItem(0).cast("double")) \
                        .drop("coordinates")

    # Rename columns to be more readable
    df_arptcodes = df_arptcodes.withColumnRenamed("ident", "airport_id") \
                            .withColumnRenamed("type", "airport_type") \
                            .withColumnRenamed("name", "airport_name") \
                            .withColumnRenamed("continent", "continent_abbrv") \
                            .withColumnRenamed("iso_country", "country_code") \
                            .withColumnRenamed("iso_region", "region_code")
    
    return df_arptcodes


