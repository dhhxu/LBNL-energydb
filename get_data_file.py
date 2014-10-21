import pyodbc, sys, os, csv, shutil

db = "energydb"
user = "danielxu"
pwd = os.getenv('ENERGYDBPASS')

# DEFAULT file paths
default_data_file_path = "/home/danielxu/data/data_files/"

def done():
    print("done")

def fail():
    print("FAIL")

def close_cnxn(my_cursor, my_cnxn):
    print("Closing cursor ...")
    my_cursor.close()
    done()
    print("Deleting cursor ...")
    del my_cursor
    done()
    print("Closing connection ...")
    my_cnxn.close()
    done()

# Returns a list of data files in the info file located at SRC_METER_FILE_PATH.
def get_info_file_list( src_meter_file_path ):
    try:
        f = open(src_meter_file_path, 'rb')
        reader = csv.reader(f)
        data_file_list = [r[5] for r in reader]
        f.close()
        del data_file_list[0]
        return data_file_list
    except IOError, file_not_found:
        print(file_not_found)
        exit()

# Returns True if the data files in DATA_FILE_DIR_PATH are accounted for in
# the info file list INFO_FILE_LIST.
# NOTE: data file names MUST be unique!
def exists_data_files( data_file_dir_path, info_file_list ):
    data_files = [ f for f in os.listdir(data_file_dir_path)
        if os.path.isfile(os.path.join(data_file_dir_path, f)) ]
    if (set(info_file_list) != set(data_files)):
        if (len(info_file_list) > len(data_files)):
            print("ERROR: Data files found do not match with the files in" \
                " the input info file.\n")
            print("Missing/incorrect files:")
            for f in info_file_list:
                if (f not in data_files):
                    print f
        elif (len(info_file_list) == len(data_files)):
            print("ERROR: Lists do not match.\n")
            print("Incorrect file names in the data file directory:")
            for f in data_files:
                if (f not in info_file_list):
                    print f
        else:
            isBad = False
            for f in info_file_list:
                if (f not in data_files):
                    print("ERROR: Data file '%s' not found in the data file" \
                        " directory" % (f))
                    isBad = True
            if (not isBad):
                return True
        print
        return False
    else:
        return True

# do work
def load_all_meter_data( info_file_list, info_file_path, data_file_dir_path ):
    for f in info_file_list:
        data_file = data_file_dir_path + f
        print data_file
        

# Load METER's information (from 
def load( meter, info_file_path, data_file ):
    print 'hello' 


# dummy
def dummy():
    print(sys.argv[1])

# Move the file from SRC to DST. 
def move(src, dst):
    print("Moving '%s' ..." % (src)),
    try:
        shutil.move(src, dst) 
        print("done")
    except IOError, missing_file_err:
        print("FAIL")
        print(missing_file_err)

    
# Usage message.
def usage():
    print("\nUsage: python %s [ FILE ]" % (sys.argv[0]))
    print("\t-- FILE contains meter information as well as the file name of" \
        "the corresponding data file")
    print("\t-- The absolute path to FILE must be entered")
    print

def main():
    arg_len = len(sys.argv)
    if (arg_len != 2):
        usage()
        exit()

    meter_info_file_path = sys.argv[1]
    if (not os.path.isfile(meter_info_file_path)):
        print("ERROR: File '%s' not found!" % (meter_info_file_path))
        exit()

    print("Using meter info file: '%s'\n" % (meter_info_file_path))
    info_file_list = get_info_file_list( meter_info_file_path )
    check = exists_data_files( default_data_file_path,
        info_file_list )
    if (check):
        print "same"
        load_all_meter_data( info_file_list, default_data_file_path )
        dummy()
    else:
        print "not same"
        exit()
        
if __name__ == "__main__":
    main()
