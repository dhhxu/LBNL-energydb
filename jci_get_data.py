import csv
import os
import pyodbc
import sys
import util

DB = util.METASYS_DB
USER = util.METASYS_USER
PASSWD = util.METASYS_PWD

def hasHeader(line):
    """
    Returns TRUE if LINE is a correct header.
    """
    if len(line) != util.DEFAULT_LINE_LEN:
        return False
    
    if (line[0] != "SourceID" or line[1] != "start_date"
        or line[2] != "end_date" or line[3] != "unit"):
        return False
    
    return True

def get_meter_list(extract_file):
    """
    Returns a list of meter information. EXTRACT_FILE must have a header row
    for this to work.
    """
    with open(extract_file, "rb") as extract_info_file:
        reader = csv.reader(extract_info_file)
        meter_list = [row for row in reader]
        if (not hasHeader(meter_list[0])):
            raise ValueError("ERROR: Incorrect/Missing header.\n")
            return
        del meter_list[0]
        return meter_list

def extract_meter_data(extract_file):
    """
    Extract meter reading data from the database as .csv files. The meters are
    from the EXTRACT_FILE.
    Also collects and writes meter metadata to a meter information file that
    will be used to import the data into the central database.
    """
    try:
        print("Using database '%s':" % (DB.upper()))
        print("Connecting to database ..."),
        cnxn_str = "DSN=%s;UID=%s;PWD=%s" % (DB, USER, PASSWD)
        cnxn = pyodbc.connect(cnxn_str)
        util.done()
    except pyodbc.Error, connect_err:
        util.fail()
        print(connect_err)
        exit()
    cursor = cnxn.cursor()
    try:
        meter_list = get_meter_list(extract_file)    
    except ValueError, badHeader:
        print(badHeader)
        usage()
        util.abort(cursor, cnxn)

    print("Begin data extraction ...\n")
    err_count = 0
    for meter_row in meter_list:
        err_count += extract(meter_row, cursor)
    print("\nExtraction finished.")
    if err_count == 0:
        print("No errors encountered.")
    else:
        print("%d of %d IDs failed to process" % (err_count, len(meter_list)))
    util.close_cnxn(cursor, cnxn)

def extract(mtr_row, my_cursor):
    """
    Extract reading data for the meter described in MTR_ROW to a csv file in the
    following format:    JCI_meterID_start_end.csv
    This method will write the information necessary to import the reading into
    the database to the header of the files above.
    Return 0 if succesful, 1 if error(s) occur.
    Uses cursor MY_CURSOR.
    """
    meter_id = int(mtr_row[0])
    start = mtr_row[1]
    end = mtr_row[2]
    unit = mtr_row[3]
    
    print("Processing Source ID: %d" % (meter_id))
    description = get_description(meter_id, my_cursor)
    if (not description
        or not get_reading_type(meter_id, my_cursor, start, end)
        or not get_commodity(unit, description)
        or not get_readings(meter_id, start, end, my_cursor)):
        util.tryNext()
        return 1

    output_filename = "JCI_%s_%s_%s.csv" % (str(meter_id), start, end)
    output_path = util.DATA_OUTPUT_FILE_PATH + output_filename

    with open(output_path, "wb") as output_file:
        writer = csv.writer(output_file, delimiter=',')
        header = [description, unit, commodity, "JCI", reading_type]
        writer.writerow(header)
        for row in my_cursor:
            writer.writerow(["NULL" if r is None else r for r in row])

    print("Processing finished.\n")
    return 0

def get_description(meter_id, my_cursor):
    """
    Returns the string containing the meter description corresponding to METER_ID
    or None if no description can be found or an error occurs. Uses cursor
    MY_CURSOR
    """
    get_description_sql = """
        SELECT TOP 1 PointName FROM tblPoint WHERE PointID = %d
    """ % (meter_id)

    print("Getting meter name ..."),
    
    try:
        my_cursor.execute(get_description_sql)
    except pyodbc.Error, meter_name_err:
        util.fail()
        print(meter_name_err)
        return None
    description_result = my_cursor.fetchone()
    if (not description_result):
        util.fail()
        print("ERROR! Description not found!")
        return None
    else:
        util.done()
        return description_result.PointName

def get_reading_type(meter_id, my_cursor, start_date, end_date):
    """
    Returns the reading type of meter METER_ID (Totalization, Interval), using
    cursor MY_CURSOR. If reading type cannot be determined, return NONE.
    Readings are limited to those between START_DATE and END_DATE
    """
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
        util.fail()
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

def isMonotonic(my_cursor):
    """
    Given a list of values stored in cursor MY_CURSOR, return TRUE if values
    are totalized (i.e. monotonic), FALSE if values are in interval form.
    """
    first = my_cursor.fetchone()
    if not first:
        util.fail()
        raise ValueError("ERROR: No meter values found in input date range")
    else:
        count = 0
        prev = first.ActualValue
        for v in my_cursor:
            curr = v.ActualValue 
            diff = curr - prev
            if (diff < 0):
                util.done()
                return False
            elif (count >= 100):
                break
            count += 1
            prev = curr
        util.done()
        return True

def get_commodity(orig_unit, desc):
    """
    Return the commodity type (Water, Gas, Electricity) based on ORIG_UNIT and
    description DESC. Return None if commodity cannot be found.
    """
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
    if (not output):
        util.fail()
        print("ERROR: No commodity found for unit '%s'" % (orig_unit))
        return None
    else:
        util.done()
        return output

def get_readings(meter_id, start_date, end_date, my_cursor):
    """
    Run the SQL to get readings for meter METER_ID between START_DATE and
    END_DATE. Return TRUE if successful, FALSE otherwise. The readings are
    stored in MY_CURSOR for later iteration.
    """
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
        util.done()
        return True
    except pyodbc.Error, get_data_err:
        util.fail()
        print(get_data_err)
        return False

def usage():
    """
    Usage message.
    """
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
