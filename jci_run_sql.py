import pyodbc, sys, os

# variables
db = "metasys"
user = "EETD"
pwd = os.getenv('KFAJCIPASS')

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
    print(my_sql)
    cursor.execute(my_sql)

    for r in cursor:
        print r


    cursor.close()
    del cursor
    cnxn.close()

if __name__ == "__main__":
    main()
