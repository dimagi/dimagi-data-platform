'''
Created on Jun 9, 2014

@author: mel

write csv files into a directory (would usually be project name) without compressing them, then COPY to postgresql

'''
from collections import OrderedDict
import csv
import itertools
import logging
from operator import itemgetter
import os

from commcare_export.writers import TableWriter, MAX_COLUMN_SIZE, SqlTableWriter
import six

from dimagi_data_platform import config


class CsvPlainWriter(TableWriter):
    def __init__(self, dir, prefix, max_column_size=MAX_COLUMN_SIZE):
        self.dir = dir
        self.prefix = prefix
        self.tables = []
        
    def __enter__(self):
        return self

    def write_table(self, table, db_cols, hstore_col_name):
        
        with  open('%s-%s.csv' % (self.prefix, table['name']), 'w') as csv_file:
            writer = csv.writer(csv_file, dialect=csv.excel)
            
            # there is probably a nicer way to do this
            csv_headings = []
            for heading in table['headings']:
                if heading in db_cols:
                    csv_headings.append(heading)
                    
            if hstore_col_name:
                csv_headings.append(hstore_col_name)
                
            print csv_headings
            print table['headings']
                
            writer.writerow(csv_headings)
            
            row_dicts = [OrderedDict(zip(table['headings'],row)) for row in table["rows"]]
            
            for row_dict in row_dicts:

                out = []
                hstore_dict = {}
                for k,v in row_dict.iteritems():
                    if k in db_cols:
                        out.append(v.encode('utf-8') if isinstance(v, six.text_type) else v)
                    else:
                        hstore_dict[k] = v
                hstore_str = ','.join("%s=>%s" % (key,val) for (key,val) in hstore_dict.iteritems())      
                
                if hstore_col_name:
                    out.append(hstore_str)
                
                writer.writerow(out)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
class PgCopyWriter(SqlTableWriter):
    
    # there are some 36 character case IDs that might be null in the first row.
    # and also some longer xmlns entries
    MIN_VARCHAR_LEN = 255
    
    def __init__(self, connection, project):
        self.project = project
        super(PgCopyWriter, self).__init__(connection)
    
    def write_table(self, table, db_cols, hstore_col_name):
        prefix = self.project
        
        csv_writer = CsvPlainWriter(config.DATA_DIR, prefix)        
        csv_writer.write_table(table, db_cols, hstore_col_name)
        
        # do copy
        conn = self.base_connection
        
        abspath = ''
        with open('%s-%s.csv' % (prefix, table['name']), 'r') as csv_file:
            abspath = os.path.abspath(csv_file.name)
            headings = csv_file.readline()
        
        delete_sql = "DELETE FROM %s WHERE domain LIKE '%s'" % (table['name'], self.project)
            
        copy_sql = "COPY %s (%s) FROM '%s' WITH CSV HEADER" % (table['name'],headings, abspath)
        
        trans = conn.begin()
        conn.execute(delete_sql)
        conn.execute(copy_sql)
        trans.commit()
        
        conn.close()
