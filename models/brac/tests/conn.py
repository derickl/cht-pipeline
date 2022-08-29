import psycopg2
from psycopg2 import Error
from vars import *

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
    # Connect to an existing database
    # conn = psycopg2.connect(
    # host="cht-pipeline-test.caseqzeqqeog.eu-west-2.rds.amazonaws.com",
    # database="cht_pipeline_test",
    # user="postgres",
    # password="Bc5Tpy34vQaxAbw");

    conn = psycopg2.connect(
    database="brac-ug",
    user="postgres",
    password="");

    # Create a cursor to perform database operations
    cursor = conn.cursor()
    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(conn.get_dsn_parameters(), "\n")
    # Executing a SQL query
    cursor.execute("SELECT version();")
    # Fetch result
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")

    # Executing a second SQL query
    for key in queries:
        cursor.execute(queries[key])
        # Fetch result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        if(record != 'None'):
            if record[1] == record[2]:
                print(bcolors.OKGREEN + "Test " + key + " passed" + bcolors.ENDC)
            else:
                print("record1: " + record[1] + " record2: " + record[2])
            print(bcolors.WARNING + "Test " + key + " failed" + bcolors.ENDC)

    # cursor.execute(query2)
    # # Fetch result
    # record = cursor.fetchone()
    # print("You are connected to - ", record, "\n")
    # if record[1] == record[2]:
    #     print(bcolors.OKGREEN + "Test get_hmis_data() passed" + bcolors.ENDC)
    # else:
    #     print(bcolors.WARNING + "Test get_hmis_data() failed" + bcolors.ENDC)

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (conn):
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")