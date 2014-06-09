'''
Created on Jun 9, 2014

@author: mel

write csv files into a directory (would usually be project name) without compressing them, then COPY to postgresql

'''
from collections import OrderedDict
import csv
import logging
import os

from commcare_export.writers import TableWriter, MAX_COLUMN_SIZE, SqlTableWriter
import six


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')


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
    
    #there are some 36 character case IDs that might be null in the first row.
    #and also some longer xmlns entries
    MIN_VARCHAR_LEN=255
    
    def __init__(self, connection, project):
        self.project = project
        super(PgCopyWriter,self).__init__(connection)
    
    def write_table(self, table):
        csv_writer = CsvPlainWriter('%s/'%self.project)
        
        table_name = table['name']
        
     
        #make headings the same as col names
        for row in table['rows']:
            first_row_dict = OrderedDict(zip(table['headings'],row))
            break

        
        self.make_table_compatible(table_name, first_row_dict)
        
        table['name'] = self.table(table_name)
        table['headings'] = [self.table(table_name).c[heading].name for heading in table['headings']]
        
        logger.debug('writing table with headings: %s' % table['headings'])
        csv_writer.write_table(table)
        
        #do copy
        conn = self.base_connection
        
        abspath = ''
        with open('%s.csv' % table['name'],'r') as csv_file:
            abspath =  os.path.abspath(csv_file.name)
        
        delete_sql = "DELETE FROM %s WHERE domain LIKE '%s'" % (table['name'],self.project)
        logger.debug('deleting before copy, table is %s, sql is %s'%(table['name'],delete_sql))
        copy_sql = "COPY %s (%s) FROM '%s' WITH CSV HEADER" % (table['name'],",".join(table['headings']),abspath)
        logger.debug('copying table %s into db with sql %s'%(table['name'],copy_sql))
        
        trans = conn.begin()
        conn.execute(delete_sql)
        conn.execute(copy_sql)
        trans.commit()
        
        conn.close()
