import pyodbc, sys, os, csv, shutil

# variables
db = "energydb"
user = "danielxu"
pwd = os.getenv('ENERGYDBPASS')

# Default data directory if none is supplied at script invocation.
DEFAULT_DATA_DIR = "/home/danielxu/data/data_files/"

# Default processed data directory if none is supplied at script invocation.
DEFAULT_PROCESSED_DIR = DEFAULT_DATA_DIR + "processed/"

# Print 'done'
def done():
    print("done")

# Print 'FAIL'
def fail():
    print("FAIL")

# Print 'Trying next meter in list ...'
def tryNext():
    print("\nTrying next meter in list ...")

# Moves file whose path is SRC to directory whose path is DST. Both SRC and
# DST must exist.
def move(src, dst):
    print("Moving file '%s' to directory '%s' ..." % (src, dst)),
    shutil.move(src, dst)
    done()

# Execute SQL_STMT with cursor MY_CURSOR and commit the changes.
# Use this for operations that write to the database.
def exec_and_commit(my_cursor, sql_stmt):
    my_cursor.execute(sql_stmt)
    my_cursor.commit()

# Drop temporary table TABLE_NAME using cursor MY_CURSOR.
def drop_tmp_table(my_cursor, table_name):
    print("Deleting temporary table '%s' ..." % (table_name)),
    dele_sql = "DROP TABLE IF EXISTS %s" % (table_name)
    exec_and_commit(my_cursor, dele_sql)
    done()

# Close connection MY_CNXN, delete and close cursor MY_CURSOR.
def close_cnxn(my_cursor, my_cnxn):
    print("Closing cursor ..."),
    my_cursor.close()
    done()
    print("Deleting cursor ..."),
    del my_cursor
    done()
    print("Closing connection ..."),
    my_cnxn.close()
    done()

# Closes connection and quits script execution in case of SQL error.
def abort(my_cursor, my_cnxn):
    close_cnxn(my_cursor, my_cnxn)
    exit()

# Return a list of the absolute path to the files in DATADIR.
def get_data_files(datadir):
    return [ os.path.join(datadir, f) for f in os.listdir(datadir)
        if os.path.isfile(os.path.join(datadir, f)) ] 

# Return DATA_FILE's header as a list. The elements of the list is as follows:
# DESCRIPTION, ORIGINAL UNIT, COMMODITY, SOURCE SYSTEM NAME, READING TYPE
# NOTE: call this ONLY once per data file.
def get_header(data_file):
    with open(data_file, "rb") as f:
        reader = csv.reader(f)
        header = reader.next()
        if (len(header) != 5):
            raise ValueError("Incorrect file header")
        return header

# Load the data files in FILE_LIST into the database with cursor MYCURSOR
# Data files are moved to PROCESS_DIR if they are
# loaded successfully.
def load_files(file_list, mycursor, process_dir):
    file_count = len(file_list)
    err_count = 0
    for f in file_list:
        print("Processing file '%s'" % (f))
        err_count += load(f, mycursor, process_dir)
    print
    if (err_count == 0):
        print("All files loaded into database.")
    else:
        print("%d of %d files could not be loaded into the database\n"
                % (err_count, file_count))

# Loads the contents of DATA_FILE's header into the 'meter' table.
# Also loads the readings in the file into the 'meter_value' table.
# Afterwards, moves DATA_FILE to PROCESS_DIR.
# Returns 0 if successful, 1 on error.
# Uses cursor MYCURSOR 
def load(data_file, mycursor, process_dir):
    try:
        header = get_header(data_file)
    except ValueError, badHeader:
        print(badHeader)
        tryNext()
        return 1
    description     = header[0]
    orig_unit       = header[1]
    commodity       = header[2]
    source          = header[3]
    reading_type    = header[4]

    try:
        print("Performing ID collection ...\n")
        unit_id             = get_id(orig_unit, "unit", "old_unit", mycursor)
        commodity_id        = get_id(commodity, "commodity", "name", mycursor)
        source_system_id    = get_id(source, "source_system", "name", mycursor)
        reading_type_id     = get_id(reading_type, "reading_type", "name"
            , mycursor)
        print("\nID collection finished.\n") 
    except pyodbc.Error, general_err:
        print("SQL ERROR!")
        print(general_err)
        tryNext()
        return 1 
    if (unit_id == -1 or commodity_id == -1 or source_system_id == -1
            or reading_type_id == -1):
        print("ERROR: Some IDs not found!")
        tryNext()
        return 1
    l = [description, unit_id, commodity_id, source_system_id, reading_type_id]

    meter_id = load_ids(l, mycursor)
    if (meter_id == -1):
        return 1
    print("Meter ID: %d" % (meter_id))

    valuesLoaded = load_values(meter_id, data_file, mycursor)
    if (not valuesLoaded):
        print("NOTE! Meter ID (%d) created for '%s'" % (meter_id, data_file))
        return 1
    else:
        move(data_file, process_dir)
        return 0

# HELPER METHODS FOR LOAD()

# Returns the ID from table TABLE whose FIELD is "ilike" to ITEM. Uses
# cursor MYCURSOR. If no ID is found, returns -1.
def get_id(item, table, field, mycursor):
    sql = "SELECT id FROM %s WHERE %s ILIKE '%s' LIMIT 1" % (table, field, item) 
    print("Getting %s ID ..." % (table)),
    mycursor.execute(sql)
    result = mycursor.fetchone()
    if (not result):
        fail()
        return -1
    else:
        done()
        return result.id

# Insert ID list ID_LIST into the 'meter' table and return the meter ID
# created by the insertion. Returns -1 in case of failure.
# The order of ID_LIST is as follows:
# [ description, unit_id, commodity_id, source_system_id, reading_type_id ]
# Uses cursor MYCURSOR.
def load_ids(id_list, mycursor):
    sql = """
        INSERT INTO meter
        (
            description,
            unit_id,
            commodity_id,
            source_system_id,
            reading_type_id
        )
        VALUES      ('%s', %d, %d, %d, %d)
        RETURNING   id
    """ % (id_list[0], id_list[1], id_list[2], id_list[3], id_list[4])
    print("Inserting ID's into 'meter' table ..."),
    try:
        exec_and_commit(mycursor, sql)
        result = mycursor.fetchone()
        done()
        return result.id
    except pyodbc.Error, get_meter_id_err:
        fail()
        print(get_meter_id_err)
        return -1

# Insert reading values into the 'meter_value' table from DATA_FILE for a meter
# with id M_ID. Returns TRUE if successful, FALSE otherwise.
# Uses cursor MYCURSOR.
def load_values(m_id, data_file, mycursor):
    print("Begin inserting readings into 'meter_value' table ...\n")
    tbl = "tmp_%d" % (m_id)
    hasTable = create_temp_table(tbl, mycursor)
    if (not hasTable):
        return False
    isCopied = copy_data(data_file, tbl, mycursor)
    if (not isCopied):
        drop_tmp_table(mycursor, tbl)
        return False
    hasCol = add_id_col(m_id, tbl, mycursor) 
    if (not hasCol):
        drop_tmp_table(mycursor, tbl)
        return False
    isLoaded = insert_table(tbl, mycursor)
    if (not isLoaded):
        drop_tmp_table(mycursor, tbl)
        return False

    print("\nReading insertion finished.\n")
    drop_tmp_table(mycursor, tbl)
    return True

# Create temporary table with name TABLE_NAME to hold timestamp, reading data 
# in the data file currently being processed.
# Returns TRUE if successful, FALSE otherwise. Uses cursor MYCURSOR.
def create_temp_table(table_name, mycursor):
    print("Creating temporary table '%s' ..." % (table_name)),
    sql = """
        CREATE TABLE IF NOT EXISTS %s
        (
            time_stamp_utc  TIMESTAMP,
            reading         NUMERIC
        )
    """ % (table_name)
    try:
        exec_and_commit(mycursor, sql)
        done()
        return True
    except pyodbc.Error, create_tbl_err:
        fail()
        print(create_tbl_err)
        return False
    
# Copy the data contents of DATA_FILE to temporary table TABLE.
# Returns TRUE if successful, FALSE otherwise. Uses cursor MYCURSOR.
def copy_data(data_file, table, mycursor):
    print("Copying data to temporary table '%s' ..." % (table)),
    sql = """
        COPY %s
        (
            time_stamp_utc,
            reading
        ) FROM '%s'
        WITH DELIMITER ','
        NULL AS 'Null'
        CSV HEADER
    """ % (table, data_file)
    try:
        exec_and_commit(mycursor, sql)
        done()
        return True
    except pyodbc.Error, copy_err:
        fail()
        print(copy_err)
        return False

# Add a 'id' column with default value M_ID to table TABLE using cursor
# MYCURSOR. Returns TRUE if successful, FALSE otherwise.
def add_id_col(m_id, table, mycursor):
    print("Adding column to table '%s' with id value %d ..." % (table, m_id)),
    sql = "ALTER TABLE %s ADD COLUMN id INTEGER DEFAULT %d" % (table, m_id)
    try:
        exec_and_commit(mycursor, sql)
        done()
        return True
    except pyodbc.Error, add_col_err:
        fail()
        print(add_col_err)
        return False

# Insert the contents of table TABLE into 'meter_value' using cursor MYCURSOR.
# Returns TRUE if successful, FALSE otherwise.
def insert_table(table, mycursor):
    print("Inserting readings into 'meter_value' table ..."),
    sql = """
        INSERT INTO meter_value (meter_id, time_stamp_utc, reading)
        SELECT id, time_stamp_utc, reading FROM %s
    """ % (table)
    try:
        exec_and_commit(mycursor, sql)
        done()
        return True
    except pyodbc.Error, insert_table_err:
        fail()
        print(insert_table_err)
        return False



# Usage file
def usage():
    print("\nUSAGE: python load_data_files.py [DIRECTORIES] ")
    print("\nDESCRIPTION:")
    print("\tLoads meter data files in a data directory into the energy")
    print("\tdatabase. After loading, the files are then moved to a ")
    print("\t'processed' directory.")
    print("\n\tThe files must have the following structure:")
    print("\tThe first line must have the following information about the")
    print("\tmeter:\n")
    print("\t\tdescription")
    print("\t\toriginal unit")
    print("\t\tcommodity")
    print("\t\tsource system name")
    print("\t\treading type")
    print("\n\tSubsequent lines must be in the following format:")
    print("\tTimestamp, Reading\n")
    print("\nOPTIONS:")
    print("\tDIRECTORIES -- [data_dir processed_dir]")
    print("\n\tDATA_DIR PROCESSED_DIR are absolute paths to the data and")
    print("\tprocessed directories, respectively")
    print("\n\tDATA_DIR contains the meter data files to import.")
    print("\tPROCESSED_DIR is where data files are moved to after they")
    print("\thave been processed.")
    print("\n\tIf no argument is provided, default data directory is:")
    print("\t'%s'" % (DEFAULT_DATA_DIR))
    print("\tDefault processed directory is:")
    print("\n\t'%s'" % (DEFAULT_PROCESSED_DIR))

def main():
    arg_len = len(sys.argv)
    if (arg_len > 3 or arg_len == 2):
        usage()
        exit()
    elif (arg_len == 3):
        data_dir = sys.argv[1]
        processed_dir = sys.argv[2]
    elif (arg_len == 1):
        data_dir = DEFAULT_DATA_DIR
        processed_dir = DEFAULT_PROCESSED_DIR
    if (not os.path.isdir(data_dir) or not os.path.isdir(processed_dir)):
        print("ERROR: directory '%s' does not exist!" % (data_dir))
        exit()
    data_files = get_data_files(data_dir)
    try:
        cnxn_str = "DSN=%s;UID=%s;PWD=%s" % (db, user, pwd)
        print("Connecting to database ..."),
        cnxn = pyodbc.connect(cnxn_str)
        done()
    except pyodbc.Error, conn_err:
        fail()
        print(conn_err)
        exit()
    cursor = cnxn.cursor()
    load_files(data_files, cursor, processed_dir)
    close_cnxn(cursor, cnxn)


if __name__ == "__main__":
    main()
