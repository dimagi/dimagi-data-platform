'''
Created on Jun 9, 2014

@author: mel

write csv files into a directory (would usually be project name) without compressing them, then COPY to postgresql

'''
import StringIO
import csv

from commcare_export.writers import TableWriter, MAX_COLUMN_SIZE, SqlTableWriter
import six


class CsvPlainWriter(TableWriter):
    def __init__(self, dir, max_column_size=MAX_COLUMN_SIZE):
        self.dir = dir
        self.tables = []
        
    def __enter__(self):
        return self

    def write_table(self, table):
        
        with  open('%s.csv' % table['name'], 'w') as csv_file:
            writer = csv.writer(csv_file, dialect=csv.excel)
            writer.writerow(table['headings'])
            for row in table['rows']:
                writer.writerow([val.encode('utf-8') if isinstance(val, six.text_type) else val
                                 for val in row])

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
class PgCopyWriter(SqlTableWriter):
    
    def write_table(self, table):
        csv_writer = CsvPlainWriter()
        
        table_name = table['name']
        
        #make headings the same as col names
        
        
        #write csv
        
        #do copy
