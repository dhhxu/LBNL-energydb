#!/usr/bin/python

import pyodbc, os, csv, sys

#variables
db = "ion"
user = "EETD"
passwd = os.getenv('KFAIONPASS')

# directory where meter data files are saved to.
data_output_file_path = "/home/danielxu/data/data_files/"

# default meter info file header length
DEFAULT_LINE_LEN = 4

# Print 'done'
def done():
    print("done")

# Print 'FAIL'
def fail():
    print("FAIL")

# Print a message indicating program is skipping over current Source ID and
# going to the next one (due to some error).
def tryNext():
    print("Going to next Source ID in input file ...")

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

# Abort -- close connection MY_CNXN, delete and close cursor MY_CURSOR,
# and exit the script.
def abort(my_cursor, my_cnxn):
    close_cnxn(my_cursor, my_cnxn)
    exit()

# Returns TRUE if LINE is a correct header.
def hasHeader(line):
    line_len = len(line)
    if (line_len != DEFAULT_LINE_LEN):
        return False
    else:
        if (line[0] != "SourceID" or line[1] != "QuantityID"
            or line[2] != "start_date" or line[3] != "end_date"):
            return False
        else:
            return True

# Returns a list of meter information. EXTRACT_FILE must have a header row
# for this to work.
def get_meter_list(extract_file):
    with open(extract_file, "rb") as extract_info_file:
        reader = csv.reader(extract_info_file)
        meter_list = [ row for row in reader ]
        if (not hasHeader(meter_list[0])):
            raise ValueError("ERROR: Incorrect/Missing header\n")
        del meter_list[0]
        return meter_list

# Extract meter reading data from the database as .csv files. The meters are
# from the EXTRACT_FILE.
# Also collects and writes meter metadata to a meter information file that
# will be used to import the data into the central database.
def extract_meter_data( extract_file ):
    try:
        print("Using database '%s':" % (db.upper()))
        print("Connecting to database ..."),
        cnxn_str = "DSN=%s;UID=%s;PWD=%s" % (db, user, passwd)
        cnxn = pyodbc.connect(cnxn_str)
        done()
    except pyodbc.Error, connect_err:
        fail()
        print(connect_err)
        exit()
    cursor = cnxn.cursor()
    try:
        meter_list = get_meter_list(extract_file)    
    except ValueError, badHeader:
        print(badHeader)
        usage()
        abort(cursor, cnxn)
    
    print("Begin data extraction ...\n")
    err_count = 0
    total = len(meter_list)
    for meter_row in meter_list:
        err_count += extract(meter_row, cursor, cnxn)
    print("\nExtraction finished.")
    if (err_count == 0):
        print("No errors encountered.")
    else:
        print("%d of %d IDs failed to process" % (err_count, total))
    close_cnxn(cursor, cnxn)

# Extract reading data for the meter described in MTR_ROW to a csv file in the
# following format:    ION_meterID_start_end.csv
# This method will write the information necessary to import the reading into
# the database to the header of the files above.
def extract(mtr_row, my_cursor, my_cnxn):
    meter_id = int(mtr_row[0])
    quantity_id = int(mtr_row[1])
    start = mtr_row[2]
    end = mtr_row[3]
    
    print("Processing Source ID: %d" % (meter_id))
    
    description = get_description(meter_id, my_cursor)
    if (description is None):
        tryNext()
        return 1
    unit = get_unit(get_quantity_name(quantity_id, my_cursor))
    if (unit is None):
        tryNext()
        return 1
    commodity = get_commodity(unit)
    if (commodity is None):
        tryNext()
        return 1
    reading_type = get_reading_type(meter_id, quantity_id, start, end
        , my_cursor)
    if (reading_type is None):
        tryNext()
        return 1
    if (not get_readings(meter_id, quantity_id, start, end)):
        tryNext()
        return 1

    output_filename = (db.upper() + "_" + str(meter_id) + "_"
        + start + "_" + end + ".csv")
    output_path = data_output_file_path + output_filename

    with open(output_path, "wb") as output_file:
        writer = csv.writer(output_file, delimiter=',')
        header = [description, unit, commodity, db.upper(), reading_type]
        writer.writerow(header)
        for row in my_cursor:
            writer.writerow(["NULL" if r is None else r for r in row])

    print("Processing finished.\n")
    return 0

# Returns the string containing the meter description corresponding to METER_ID
# or None if no description can be found or an error occurs. Uses cursor
# MY_CURSOR
def get_description(meter_id, my_cursor):
    get_description_sql = """
        SELECT TOP 1 Name FROM Source WHERE ID = %d
    """ % (meter_id)
    print("Getting meter name ..."),
    try:
        my_cursor.execute(get_description_sql)
    except pyodbc.Error, meter_name_err:
        fail()
        print(meter_name_err)
        return None
    description_result = my_cursor.fetchone()
    if (not description_result):
        fail()
        print("ERROR! Description not found!")
        return None
    else:
        done()
        return description_result.Name

# Returns the string containing the quantity name for QUANTITY_ID, or None if
# the name cannot be found. Uses cursor MY_CURSOR.
def get_quantity_name(quantity_id, my_cursor):
    get_quantity_name_sql = """
        SELECT TOP 1 Name FROM Quantity WHERE ID = %d
    """ % (quantity_id)
    try:
        my_cursor.execute(get_quantity_name_sql)
    except pyodbc.Error, get_qty_name_err:
        print(get_qty_name_err)
        return None
    quantity_result = my_cursor.fetchone()
    if (not quantity_result):
        print("ERROR: Quantity ID %d does not exist!" % (quantity_id))
        return None
    else:
        return quantity_result.Name.lower() 

# Returns the unit string corresponding to QUANTITY_NAME, or None if an error
# occurs during name extraction.
def get_unit(quantity_name):
    if (quantity_name is None):
        return None
    else:
        if ("power" in quantity_name):
            unit = "kW"
        elif ("energy" in quantity_name):
            unit = "kWh"
        else:
            unit = "unknown"
        return unit

# Return the commodity corresponding to UNIT, or NONE if it cannot be found.
def get_commodity(unit):
    if (unit == "kW" or unit == "kWh"):
        commodity = "Electricity"
    else:
        return None
    return commodity

# Return the reading type for meter METER_ID with quantity id QUANTITY_ID
# between START_DATE and END_DATE (Totalization, Interval) or NONE if an error
# occurs. Uses cursor MY_CURSOR.
def get_reading_type(meter_id, quantity_id, start_date, end_date, my_cursor):
    monotonic_sql = """
        SELECT
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
            AS IsMonotone
        FROM (
            SELECT TOP 100 ROW_NUMBER() OVER (ORDER BY TimestampUTC)
            AS RowNum, Value
            FROM DataLog2
            WHERE SourceID = %d AND QuantityID = %d
            AND TimestampUTC >= CAST('%s' AS datetime2)
            AND TimestampUTC < CAST('%s' AS datetime2)
        ) T1 INNER JOIN (
            SELECT TOP 100 ROW_NUMBER() OVER (ORDER BY Value)
            AS RowNum, Value
            FROM DataLog2
            WHERE SourceID = %d AND QuantityID = %d
            AND TimestampUTC >= CAST('%s' AS datetime2)
            AND TimestampUTC < CAST('%s' AS datetime2)
        ) T2 ON T1.RowNum = T2.RowNum
            WHERE T1.Value <> T2.Value
    """ % (meter_id, quantity_id, start_date, end_date,
            meter_id, quantity_id, start_date, end_date)
    print("Getting reading type ..."),
    try:
        my_cursor.execute(monotonic_sql)
    except pyodbc.Error, monotonic_err:
        fail()
        print(monotonic_err)
        return None
    monotoneResult = my_cursor.fetchone().IsMonotone
    if (monotoneResult is None):
        fail()
        print("ERROR: Reading type cannot be determined")
        return None
    elif (monotoneResult == 0):
        reading_type = "Interval"
    else:
        reading_type = "Totalization"
    done()
    return reading_type

# Run the SQL to get readings for meter METER_ID with QUANTITY_ID between
# START_DATE and END_DATE. Return TRUE if successful, FALSE otherwise.
# The readings are stored in MY_CURSOR for later iteration.
def get_readings(meter_id, quantity_id, start_date, end_date):
    get_data_sql = """
        SELECT TimestampUTC, Value
        FROM DataLog2
        WHERE TimestampUTC >= CAST('%s' AS datetime2)
        AND TimestampUTC < CAST('%s' AS datetime2)
        AND SourceID = %d
        AND QuantityID = %d
        ORDER BY TimestampUTC ASC
    """ % (start_date, end_date, meter_id, quantity_id)
    print("Getting meter readings ..."),
    try:
        my_cursor.execute(get_data_sql)
        done()
        return True
    except pyodbc.Error, get_data_err:
        fail()
        print(get_data_err)
        return False

# Usage message.
def usage():
    print("\nUsage: python %s [ FILE.csv ]" % (sys.argv[0]))
    print("    -- FILE.csv contains a list of ION meter information needed ")
    print("       to extract reading data from the ION datasource\n")
    print("    -- The absolute path to FILE.csv must be specified.")
    print("\nThe structure of FILE.csv is as follows:")
    print("File must have a header line EXACTLY as follows:")
    print("SourceID, QuantityID, start_date, end_date")
    print("Rows in this file must be in the form dictated by the header " \
        "above")
    print

# Handle script execution.
def main():
    arg_len = len(sys.argv)
    if (arg_len != 2):
        usage()
        exit()
    extract_info_file = sys.argv[1] 
    if (not os.path.isfile(extract_info_file)):
        print("ERROR: file '%s' not found!\n" % (extract_info_file))
        exit()
    else:
        print("\nUsing file '%s' ...\n" % (extract_info_file))
    extract_meter_data( extract_info_file )

if __name__ == "__main__":
    main()
