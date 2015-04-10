import pyodbc, sys, os, csv

# variables
db = "ion"
user = "EETD"
pwd = os.getenv('KFAIONPASS')

def usage():
    print("USAGE:\n\tpython run_sql.py [YOUR SQL file]\n")

def main():
    if len(sys.argv) < 2:
        usage()
        exit()
    cnxn_str = "DSN=%s;UID=%s;PWD=%s" % (db, user, pwd)
    cnxn = pyodbc.connect(cnxn_str)
    cursor = cnxn.cursor()

    f = open(sys.argv[1], 'r')
    my_sql = f.read()
    f.close()
    #print(my_sql)
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
