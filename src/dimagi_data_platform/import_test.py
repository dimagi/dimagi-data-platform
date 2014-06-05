'''
Created on Jun 5, 2014

@author: mel
'''


import getpass
import json
import logging
from commcare_export.minilinq import *
from commcare_export.commcare_hq_client import CommCareHqClient
from commcare_export.commcare_minilinq import CommCareHqEnv
from commcare_export.env import BuiltInEnv, JsonPathEnv
from commcare_export.writers import JValueTableWriter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, 
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

api_client = CommCareHqClient('https://www.commcarehq.org','melissa-test-project').authenticated('melissa.loudon@gmail.com', getpass.getpass())
form_query = Emit(table='form', 
                   headings=[Literal('id'),
                             Literal('xmlns'),
                             Literal('app_id'),
                             Literal('domain'),
                             Literal('appVersion'),
                             Literal('deviceID'),
                             Literal('userID'),
                             Literal('is_phone_submission'),
                             Literal('timeStart'),
                             Literal('timeEnd'),
                             Literal('received_on')],
                   source=Map(source=Apply(Reference('api_data'),Literal('form')),
                              body=List([Reference('id'),
                             Reference('form.@xmlns'),
                             Reference('form.@app_id'),
                             Reference('domain'),
                             Reference('metadata.appVersion'),
                             Reference('metadata.deviceID'),
                             Reference('metadata.userID'),
                             Reference('is_phone_submission'),
                             Reference('metadata.timeStart'),
                             Reference('metadata.timeEnd'),
                             Reference('received_on')])))

writer = JValueTableWriter()

env = BuiltInEnv() | CommCareHqEnv(api_client) | JsonPathEnv({})
results = form_query.eval(env)

if len(list(env.emitted_tables())) > 0:
        with writer:
            for table in env.emitted_tables():
                logger.debug('Writing %s', table['name'])
                writer.write_table(table)
                print(json.dumps(writer.tables, indent=4, default=RepeatableIterator.to_jvalue))
                
else:
    logger.warn('Nothing emitted')