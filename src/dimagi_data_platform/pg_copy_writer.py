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

from dimagi_data_platform import config


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')


class CsvPlainWriter(TableWriter):
    def __init__(self, dir, prefix, max_column_size=MAX_COLUMN_SIZE):
        self.dir = dir
        self.prefix = prefix
        self.tables = []
        
    def __enter__(self):
        return self

    def write_table(self, table):
        
        with  open('%s-%s.csv' % (self.prefix, table['name']), 'w') as csv_file:
            writer = csv.writer(csv_file, dialect=csv.excel)
            writer.writerow(table['headings'])
            for row in table['rows']:
                writer.writerow([val.encode('utf-8') if isinstance(val, six.text_type) else val
                                 for val in row])

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
class PgCopyWriter(SqlTableWriter):
    
    # there are some 36 character case IDs that might be null in the first row.
    # and also some longer xmlns entries
    MIN_VARCHAR_LEN = 255
    
    def __init__(self, connection, project):
        self.project = project
        super(PgCopyWriter, self).__init__(connection)
    
    def make_table_compatible(self, table_name, row_dict):
        # FIXME: This does lots of redundant checks in a tight loop. Stop doing that.
        
        ctx = self.alembic.migration.MigrationContext.configure(self.connection)
        op = self.alembic.operations.Operations(ctx)

        if not table_name in self.metadata.tables:
            
            if 'id' in row_dict:
                id_column = self.sqlalchemy.Column(
                    'id',
                    self.sqlalchemy.Unicode(self.MAX_VARCHAR_LEN),
                    primary_key=True
                )
            else:  # we need an auto-generated ID col
                logger.debug('adding an autogenerated ID column to table %s' % table_name)
                id_column = self.sqlalchemy.Column(
                    'id',
                    self.sqlalchemy.INTEGER,
                    autoincrement=True,
                    primary_key=True
                )
            op.create_table(table_name, id_column)
            self.metadata.reflect()

        for column, val in row_dict.items():
            ty = self.best_type_for(val)

            if not column in [c.name for c in self.table(table_name).columns]:
                # If we are creating the column, a None crashes things even though it is the "empty" type
                # but SQL does not have such a type. So we have to guess a liberal type for future use.
                ty = ty or self.sqlalchemy.UnicodeText()
                op.add_column(table_name, self.sqlalchemy.Column(column, ty, nullable=True))
                self.metadata.clear()
                self.metadata.reflect()

            else:
                columns = dict([(c.name, c) for c in self.table(table_name).columns])
                current_ty = columns[column].type

                if not self.compatible(ty, current_ty) and not ('sqlite' in self.connection.engine.driver):
                    op.alter_column(table_name, column, type_=self.least_upper_bound(current_ty, ty))
                    self.metadata.clear()
                    self.metadata.reflect()

    
    def write_table(self, table):
        prefix = self.project
        csv_writer = CsvPlainWriter(config.DATA_DIR, prefix)
        
        table_name = table['name']
        
     
        # make headings the same as col names
        for row in table['rows']:
            first_row_dict = OrderedDict(zip(table['headings'], row))
            break

        
        self.make_table_compatible(table_name, first_row_dict)
        
        table['name'] = self.table(table_name)
        table['headings'] = [self.table(table_name).c[heading].name for heading in table['headings']]
        
        logger.debug('writing table with headings: %s' % table['headings'])
        csv_writer.write_table(table)
        
        # do copy
        conn = self.base_connection
        
        abspath = ''
        with open('%s-%s.csv' % (prefix, table['name']), 'r') as csv_file:
            abspath = os.path.abspath(csv_file.name)
        
        delete_sql = "DELETE FROM %s WHERE domain LIKE '%s'" % (table['name'], self.project)
        logger.debug('deleting before copy, table is %s, sql is %s' % (table['name'], delete_sql))
        copy_sql = "COPY %s (%s) FROM '%s' WITH CSV HEADER" % (table['name'], ",".join(table['headings']), abspath)
        logger.debug('copying table %s into db with sql %s' % (table['name'], copy_sql))
        
        trans = conn.begin()
        conn.execute(delete_sql)
        conn.execute(copy_sql)
        trans.commit()
        
        conn.close()