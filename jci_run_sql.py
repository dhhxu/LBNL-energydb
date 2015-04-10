#!/usr/bin/env python

"""
Runs the SQL query from a user-supplied input file on the JCI database and
prints the results.
"""

import pyodbc
import sys
import os
import util

db = util.METASYS_DB
user = util.METASYS_USER
pwd = util.METASYS_PWD

def usage():
    print("USAGE:\n\tpython jci_run_sql.py [YOUR SQL file]\n")

def main():
    if len(sys.argv) < 2:
        usage()
        exit()
    cnxn_str = "DSN=%s;UID=%s;PWD=%s" % (db, user, pwd)
    cnxn = pyodbc.connect(cnxn_str)
    cursor = cnxn.cursor()

    f = open(sys.argv[1], 'r')
    my_sql = f.read()
    cursor.execute(my_sql)

    for r in cursor:
        print(r)

    cursor.close()
    del cursor
    cnxn.close()

if __name__ == "__main__":
    main()
