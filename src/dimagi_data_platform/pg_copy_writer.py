'''
Created on Jun 9, 2014

@author: mel

write csv files for a set of database columns, as well as an optional hstore column for addition properties, then COPY to postgresql

'''
from collections import OrderedDict
import csv
import os

from commcare_export.writers import TableWriter, SqlTableWriter
import six

import config


class CsvPlainWriter(TableWriter):
    def __init__(self, dir):
        self.dir = dir
        self.tables = []
        
    def __enter__(self):
        return self

    def write_table(self, table, csvfilename, db_cols, hstore_col_name):
        
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
            
        filepath = os.path.join(self.dir, csvfilename)
    
        with  open(filepath, 'w') as csv_file:
            writer = csv.writer(csv_file, dialect=csv.excel)
            
            # there is probably a nicer way to do this
            csv_headings = []
            for heading in table['headings']:
                if heading in db_cols:
                    csv_headings.append(heading)
                    
            if hstore_col_name:
                csv_headings.append(hstore_col_name)
                
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
    
    
    def __init__(self, connection, project):
        self.project = project
        super(PgCopyWriter, self).__init__(connection)
    
    def write_table(self, table, db_cols, hstore_col_name):
        prefix = self.project
        
        csvdir = config.TMP_FILES_DIR
        csvfilename = '%s-%s.csv' % (prefix, table['name'])
        
        csv_writer = CsvPlainWriter(csvdir)        
        csv_writer.write_table(table, csvfilename, db_cols, hstore_col_name)
        
        csvfile = os.path.join(csvdir, csvfilename)
        with open(csvfile, 'r') as csv_file:
            abspath = os.path.abspath(csv_file.name)
            headings = csv_file.readline()
        
        conn = self.base_connection
        
        delete_sql = "DELETE FROM %s WHERE domain LIKE '%s'" % (table['name'], self.project)
        copy_sql = "COPY %s (%s) FROM '%s' WITH CSV HEADER" % (table['name'],headings, abspath)
        
        trans = conn.begin()
        conn.execute(delete_sql)
        conn.execute(copy_sql)
        trans.commit()
        
        conn.close()
