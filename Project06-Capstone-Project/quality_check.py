from pyspark.sql.functions import *



def has_records(df_spark, tbl_name):
    
    """
        Description:
            Checks if there are records written to the table. If there are greater than 0
            records, the check passes, a message is printed and True is returned.
            If there are 0 records, the check fails, a message is printed and False is
            returned.  Also checks for invalid state where record count is less than zero,
            in which case a message is printed and a None (null) is returned.
            
        Arguments:
            df_spark: The dataframe containing table data
            tbl_name: The name of the table being checked, used for generating messages
        
        Return:
            check_state: The result of whether the check has passed or failed
    """
    
    # Get record count
    rec_count = df_spark.count()
    
    # Create boolean check variable, done for return data type consistency, even when null
    check_state = bool()
    
    # Check record counts for pass fail
    if rec_count > 0:
        print("Check Passed in table {} with record count: {}".format(rec_count, tbl_name))
        check_state = True
    else:
        if rec_count == 0:
            print("Count check failed with no records in table: {}".format(tbl_name))
            check_state = False
        elif rec_count < 0:
            print("Invalid State with Negative value: Showing {} records for tbl_name".format(rec_count, tbl_name))
            check_state = None
        
    return check_state



def check_prikey_null(df_spark, tbl_name, columns):
    
    """
        Description:
            This function checks if there are nulls in one or more primary
            key fields.  A string is used when checking one column and a
            list is used when checking more than one column.
        
        Arguments:
            df_spark: The dataframe containing table data
            tbl_name: The name of the table being checked, used for generating messages
            columns: One or more columns to be checked for nulls
        
        Return:
            null_check_pass: Returns True for Pass or False for Fail
    """
    
    # Handle 1 or more columns (multiple columns are a list)
    if isinstance(columns, str) == True:
        
        # Filter and get column count
        null_count = df_spark.filter(col(columns).isNull()).count()
        
    else:
        
        # Get first column for query
        qry_str = columns.pop(0) + " IS NULL"
        
        # Get each column for the second and latter columns for query
        for column in columns:
            qry_str += " AND {} IS NULL".format(column)
        
        # Get null count
        null_count = df_spark.filter(qry_str).count()
    
    # Check if there are nulls in the primary key column(s)
    if null_count == 0:
        print("Primary Key Null check passed for Table: {}".format(tbl_name))
        null_check_pass = True
    else:
        print("Primary Key Null check failed Null in table {} with Count: {}".format(tbl_name, null_count))
        null_check_pass = False
    
    return null_check_pass