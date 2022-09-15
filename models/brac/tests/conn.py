import sys
import time
import psycopg2
from psycopg2 import Error
from dbsettings import *
from vars import *

start = time.time()

foundCounter = 0

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


try:
    cursor = conn.cursor()
    cursor2 = conn2.cursor()
    # Fetch result
    
    for key in queries:
        print(queries[key])
    
        cursor.execute(queries[key])
        cursor2.execute(queries_2[key])

        # Fetch result
        records = cursor.fetchall()
        arrayHaystack = cursor2.fetchall()
        
        print("Query has been fetched succesfully")
        for index, key_record in enumerate(records):
            # print(record)
            print("CHW: ", key_record[0]," MD5: ",  key_record[2])
            result = search_hash(key_record[2], arrayHaystack, foundCounter)
            arrayHaystack = result[0]
            foundCounter = result[1]
    
    print(foundCounter, " out of ", len(records), " records from Brac-ug DB match cht_pipeline_test dbt DB" )
    print(f'Time taken: {time.time() - start}')

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (conn):
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
