"""
Contains useful variables and methods used by the LBNL building energy
datbase scripts.
"""

import os
import shutil

# Metasys database
METASYS_DB = "metasys"
METASYS_USER = "EETD"
METASYS_PWD = os.getenv('KFAJCIPASS')

# ION database
ION_DB = "ion"
ION_USER = "EETD"
ION_PWD= os.getenv('KFAIONPASS')

# Energy database
PG_DB = "energydb"
PG_USER = "danielxu"
PG_PWD = os.getenv('ENERGYDBPASS')

# Directory where meter data files are saved to.
DATA_OUTPUT_FILE_PATH = "/home/danielxu/data/data_files/"

# Default meter info file header length
DEFAULT_LINE_LEN = 4



def done():
    """
    Print 'done'
    """
    print("done")

def fail():
    """
    Print 'FAIL'
    """
    print("FAIL")

def close_cnxn(my_cursor, my_cnxn):
    """
    Close connection MY_CNXN, delete and close cursor MY_CURSOR.
    """
    print("Closing cursor ..."),
    my_cursor.close()
    done()
    print("Deleting cursor ..."),
    del my_cursor
    done()
    print("Closing connection ..."),
    my_cnxn.close()
    done()

def abort(my_cursor, my_cnxn):
    """
    Abort -- close connection MY_CNXN, delete and close cursor MY_CURSOR,
    and exit the script.
    """
    close_cnxn(my_cursor, my_cnxn)
    exit()

def tryNext():
    """
    Print a message indicating program is skipping over current Source ID and
    going to the next one (due to some error).
    """
    print("Going to next Source ID in input file ...")

def move(src, dst):
    """
    Moves file whose path is SRC to directory whose path is DST. Both SRC and
    DST must exist.
    """
    print("Moving file '%s' to directory '%s' ..." % (src, dst)),
    shutil.move(src, dst)
    done()
