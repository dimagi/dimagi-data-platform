Data pull code for Dimagi's internal data platform.

For each data source, the data pull comprises the following steps:
 * An Extracter extracts the data from the source - for example by reading a spreadsheet or accessing an API. The extracted data is saved, with few modifications, in an incoming data table.
 * A Loader loads the extracted data into the normalized database tables - the canonical form of data in the data warehouse.

To run, you'll need
 - A CommCareHQ admin account
 - A list of domains downloaded from HQ (see sample_input/domains.xslx)
 - Config files config_run and config_system.json (see samples in sample_configs/) in the same directory as run_import.py
