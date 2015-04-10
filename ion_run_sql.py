"""
Runs the user-supplied SQL query on the ION database and writes the results
to 'ion_sql_output.csv'.
"""

import csv
import pyodbc
import sys
import util

db = util.ION_DB
user = util.ION_USER
pwd = util.ION_PWD

def usage():
    print("USAGE:\n\tpython ion_run_sql.py [YOUR SQL file]\n")

def main():
    if len(sys.argv) < 2:
        usage()
        exit()

    cnxn_str = "DSN=%s;UID=%s;PWD=%s" % (db, user, pwd)
    cnxn = pyodbc.connect(cnxn_str)
    cursor = cnxn.cursor()

    with open(sys.argv[1], 'r') as inp:
        my_sql = inp.read()

    cursor.execute(my_sql)

    if (cursor):
        with open("ion_sql_output.csv", "wb") as f:
            writer = csv.writer(f)
            for r in cursor:
                writer.writerow(r)
    else:
        print("SQL query returned no results.\n")

    cursor.close()
    del cursor
    cnxn.close()

if __name__ == "__main__":
    main()
