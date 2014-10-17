'''
Created on Jun 9, 2014

@author: mel

write csv files for a set of database columns, as well as an optional hstore column for addition properties, then COPY to postgresql

'''
from collections import OrderedDict
import csv
from datetime import datetime
import logging
import os

from commcare_export.writers import TableWriter, SqlTableWriter
import six
import sqlalchemy
from sqlalchemy.orm.session import sessionmaker

from dimagi_data_platform import conf


logger = logging.getLogger(__name__)

database = conf.PEEWEE_DB_CON

class EmptyTableException(Exception):
    pass

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
            csv_headings.append('imported')
                
            writer.writerow(csv_headings)
            
            row_dicts = [OrderedDict(zip(table['headings'], row)) for row in table["rows"]]
            if len(row_dicts) == 0:
                raise EmptyTableException('Table has no rows')
            
            for row_dict in row_dicts:
                out = []
                hstore_dict = {}
                
                for k, v in row_dict.iteritems():
                    if k in db_cols:
                        out.append(v.encode('utf-8') if isinstance(v, six.text_type) else v)
                    else:
                        hstore_dict[k] = v
                hstore_str = ','.join("%s=>%s" % (key, val) for (key, val) in hstore_dict.iteritems())      
                
                if hstore_col_name:
                    out.append(hstore_str)
                
                # imported
                out.append(False)
                
                writer.writerow(out)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
class PgCopyWriter(SqlTableWriter):
    
    
    def __init__(self, connection, project):
        self.project = project
        super(PgCopyWriter, self).__init__(connection)
    
    def write_table(self, table, db_cols, hstore_col_name):
        logger.debug('pg_copy_writer starting for project %s, table %s' % (self.project,table['name']))
        prefix = self.project
        
        csvdir = conf.TMP_FILES_DIR
        csvfilename = '%s-%s.csv' % (prefix, table['name'])
        
        csv_writer = CsvPlainWriter(csvdir)        
        
        try:
            csv_writer.write_table(table, csvfilename, db_cols, hstore_col_name)
        except EmptyTableException:
            logger.info('pg_copy_writer stopping, no rows to write to csv file')
            return
        
        csvfile = os.path.join(csvdir, csvfilename)
        with open(csvfile, 'r') as csv_file:
            abspath = os.path.abspath(csv_file.name)
            headings = csv_file.readline()
        
        engine = sqlalchemy.create_engine(conf.SQLALCHEMY_DB_URL)
        session = sessionmaker(bind=engine)
        
        copy_file = open(abspath,'r')
        copy_sql = "COPY %s (%s) FROM STDIN DELIMITER ',' CSV HEADER" % (table['name'], headings)
        
        logger.debug('starting pg_copy')
        # need to use raw psycopg here to copy from stdin because with RDS not allowing admin users, can't copy from a file.
        raw_conn = engine.raw_connection()
        raw_cur = raw_conn.cursor()
        raw_cur.copy_expert(copy_sql, copy_file)
        raw_conn.commit()
        raw_cur.close()
        raw_conn.close()
