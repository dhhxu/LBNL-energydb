#!/usr/bin python

import pyodbc, os, csv, sys

#variables
db = "metasys"
user = "EETD"
passwd = os.getenv('KFAJCIPASS')

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
def hasHeader( line ):
    line_len = len(line)
    if (line_len != DEFAULT_LINE_LEN):
        return False
    else:
        if (line[0] != "SourceID" or line[1] != "start_date"
            or line[2] != "end_date" or line[3] != "unit"):
            return False
        else:
            return True

# Returns a list of meter information. EXTRACT_FILE must have a header row
# for this to work.
def get_meter_list( extract_file ):
    with open(extract_file, "rb") as extract_info_file:
        reader = csv.reader(extract_info_file)
        meter_list = [ row for row in reader ]
        if (not hasHeader(meter_list[0])):
            raise ValueError("ERROR: Incorrect/Missing header\n")
            return
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
        err_count += extract(meter_row, cursor)
    print("\nExtraction finished.")
    if (err_count == 0):
        print("No errors encountered.")
    else:
        print("%d of %d IDs failed to process" % (err_count, total))
    close_cnxn(cursor, cnxn)

# Extract reading data for the meter described in MTR_ROW to a csv file in the
# following format:    JCI_meterID_start_end.csv
# This method will write the information necessary to import the reading into
# the database to the header of the files above.
# Return 0 if succesful, 1 if error(s) occur.
# Uses cursor MY_CURSOR.
def extract(mtr_row, my_cursor):
    meter_id = int(mtr_row[0])
    start = mtr_row[1]
    end = mtr_row[2]
    unit = mtr_row[3]
    
    print("Processing Source ID: %d" % (meter_id))
    description = get_description(meter_id, my_cursor)
    if (description is None):
        tryNext()
        return 1
    reading_type = get_reading_type(meter_id, my_cursor, start, end)
    if (reading_type is None):
        tryNext()
        return 1
    commodity = get_commodity(unit, description)
    if (commodity is None):
        tryNext()
        return 1
    if (not get_readings(meter_id, start, end, my_cursor)):
        tryNext()
        return 1

    output_filename = ("JCI_" + str(meter_id) + "_"
        + start + "_" + end + ".csv")
    output_path = data_output_file_path + output_filename

    with open(output_path, "wb") as output_file:
        writer = csv.writer(output_file, delimiter=',')
        header = [description, unit, commodity, "JCI", reading_type]
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
        SELECT TOP 1 PointName FROM tblPoint WHERE PointID = %d
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
        return description_result.PointName

# Returns the reading type of meter METER_ID (Totalization, Interval), using
# cursor MY_CURSOR. If reading type cannot be determined, return NONE.
# Readings are limited to those between START_DATE and END_DATE
def get_reading_type(meter_id, my_cursor, start_date, end_date):
    check_sql = """
        SELECT TOP 100 ActualValue FROM tblActualValueFloat
        WHERE PointSliceID = %d
        AND UTCDateTime >= CAST('%s' AS datetime)
        AND UTCDateTime < CAST('%s' AS datetime)
        ORDER BY UTCDateTime ASC
    """ % (meter_id, start_date, end_date)
    print("Getting reading type ..."),
    try:
        my_cursor.execute(check_sql)
    except pyodbc.Error, monotonic_err:
        fail()
        print(monotonic_err)
        return None
    try:
        if (isMonotonic(my_cursor)):
            return "Totalization"
        else:
            return "Interval"
    except ValueError, empty_value_err:
        print(empty_value_err) 
        return None

# Given a list of values stored in cursor MY_CURSOR, return TRUE if values
# are totalized (i.e. monotonic), FALSE if values are in interval form.
def isMonotonic(my_cursor):
    first = my_cursor.fetchone()
    if (not first):
        fail()
        raise ValueError("ERROR: No meter values found in input date range")
    else:
        count = 0
        prev = first.ActualValue
        for v in my_cursor:
            curr = v.ActualValue 
            diff = curr - prev
            if (diff < 0):
                done()
                return False
            elif (count >= 100):
                break
            count += 1
            prev = curr
        done()
        return True

# Return the commodity type (Water, Gas, Electricity) based on ORIG_UNIT and
# description DESC. Return None if commodity cannot be found.
def get_commodity(orig_unit, desc):
    print("Getting commodity ..."),
    unit = orig_unit.lower()
    output = ""
    if (unit == "kw" or unit == "kwh"):
        output =  "Electricity"
    elif (unit == "cu ft" or unit == "btu"):
        output = "Gas"
    elif (unit == "gal"):
        output =  "Water"
    elif (unit == "unknown"):
        if ("btu" in desc.lower()):
            output = "Gas"
    if (output == ""):
        fail()
        print("ERROR: No commodity found for unit '%s'" % (orig_unit))
        return None
    else:
        done()
        return output

# Run the SQL to get readings for meter METER_ID between START_DATE and
# END_DATE. Return TRUE if successful, FALSE otherwise. The readings are
# stored in MY_CURSOR for later iteration.
def get_readings(meter_id, start_date, end_date, my_cursor):
    get_data_sql = """
        SELECT UTCDateTime, ActualValue
        FROM tblActualValueFloat
        WHERE UTCDateTime >= CAST('%s' AS datetime)
        AND UTCDateTime < CAST('%s' AS datetime)
        AND PointSliceID = %d
        ORDER BY UTCDateTime ASC
    """ % (start_date, end_date, meter_id)
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
    print("\nUSAGE:  python %s [ FILE.csv ]" % (sys.argv[0]))
    print("\n\tGiven a file containing JCI meter information, this script")
    print("\textracts reading data into .csv files.")
    print("\nDESCRIPTION")
    print("\tFILE.csv contains a list of JCI meter information needed ")
    print("\tto extract reading data from the JCI datasource.\n")
    print("\tThe absolute path to FILE.csv must be specified.")
    print("\n\tFile must have a header line EXACTLY as follows:")
    print("\n\t\tSourceID, start_date, end_date, unit")
    print("\n\tRows in this file must be in the form dictated by the header " \
        "above\n")

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
