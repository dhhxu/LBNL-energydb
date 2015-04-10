#LBNL Building Energy Database Project


**Script directory:** ~/scripts
**Data directory:** ~/data

##Main scripts:

1. `ion_get_data.py` (getter)

   Given an input file of information on desired meters, pulls meter data
   from the ION database.

2. `jci_get_data.py` (getter)

   Same as above script, except for JCI meters. Note that the input file
   has a different structure (details found in usage)

3. `load_data_files.py` (loader)

   Loads data pulled from the JCI and ION data getter scripts into the
   Postgres database holding building energy data. Note that the extracted
   data is of a specific format independent of database source.

###Usage:

Run the getter script(s) to get desired data. Next run the loader script
to import the extracted data into the Postgres database.

It is assumed that the information provided in the input files
for the getter scripts pertain to new meters, i.e. meters for which the
data is being pulled for the first time. Furthermore, the time period
for meter readings must be specified in advance.

Example usage:
`ion_get_data.py YOUR_INFO_FILE.csv`
`load_data_files.py` (using default arguments)

---
## Other scripts:

* `ion_run_sql.py`
* `jci_run_sql.py`

###Description:

Both scripts simply run a SQL statement contained in an input file on the
corresponding database. The full path to the SQL file must be specified.
Generally, the input files are located in the ~/sql directory.

Example usage: `python ion_run_sql.py YOUR_SQL_FILE.sql`


**Last updated on:** 2015-04-10
**Author:** Daniel Xu (danielxu@lbl.gov)