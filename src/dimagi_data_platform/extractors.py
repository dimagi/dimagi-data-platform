'''
Created on Jun 6, 2014

@author: mel
'''
import datetime
import json
import logging
import os

from commcare_export.commcare_hq_client import MockCommCareHqClient
from commcare_export.commcare_minilinq import CommCareHqEnv
from commcare_export.env import BuiltInEnv, JsonPathEnv
from commcare_export.minilinq import Emit, Literal, Map, FlatMap, Reference, \
    Apply, List
from pandas.core.common import notnull
from pandas.io.excel import ExcelFile
from playhouse.postgres_ext import HStoreField
from requests.auth import HTTPDigestAuth
from simple_salesforce.api import Salesforce, SalesforceMalformedRequest
import slumber
import sqlalchemy

import conf
from dimagi_data_platform.data_warehouse_db import Domain, DeviceLog, \
    HQExtractLog
from dimagi_data_platform.incoming_data_tables import IncomingForm, \
    IncomingCases, IncomingUser, IncomingDeviceLog, IncomingWebUser, \
    IncomingFormDef, IncomingSalesforceRecord, IncomingDomain, \
    IncomingApplication
from dimagi_data_platform.pg_copy_writer import PgCopyWriter


logger = logging.getLogger(__name__)

class Extractor(object):
    '''
    Knows how to read data from a source and write to incoming data tables.
    '''
    _incoming_table_class = None

    def __init__(self, _incoming_table_class):
        '''
        Constructor
        '''
        self._incoming_table_class = _incoming_table_class
        
        
    @property
    def _get_table_name(self):
        return self._incoming_table_class._meta.db_table
    
    @property   
    def _get_field_names(self):
        return [v for v in self._incoming_table_class._meta.fields.keys]
    
    @property   
    def _get_db_cols(self):
        return [v.db_column for v in self._incoming_table_class._meta.fields.values()]
    
    @property   
    def _get_attribute_db_cols(self):
        '''
        returns the names of all columns except the hstores
        '''
        cols = [v.db_column for k, v in self._incoming_table_class._meta.fields.iteritems() if not isinstance(v, HStoreField)]
        return cols
    
    @property       
    def _get_hstore_db_col(self):
        '''
        returns the name of the first hstore col found in the table.
        there should only be one - it's just a place to dump attributes we don't have a column for
        '''
        hstorecols = [v.db_column for k, v in self._incoming_table_class._meta.fields.iteritems() if isinstance(v, HStoreField)]
        if hstorecols:
            return hstorecols[0]
        
    def extract(self):
        self.do_extract()
        
    def cleanup(self):
        self.do_cleanup()
        
    def do_extract(self):
        raise NotImplementedError("Subclass must implement do_extract")
    
    def do_cleanup(self):
        raise NotImplementedError("Subclass must implement do_cleanup")

class CommCareExportExtractor(Extractor):
    '''
    An extractor that uses commcare-export
    '''

    def __init__(self, incoming_table_class, domain, chunked_by_date = False, chunked_by_days = 30, incremental = True):
        '''
        Constructor
        '''
        # TODO should deal in domain objects only, not domain names
        self.domain = domain
        d = Domain.get(name=self.domain)
        self.extract_log = HQExtractLog(extractor = self.__class__.__name__, domain = d)
        self.chunked_by_date = chunked_by_date
        self.chunked_by_days = chunked_by_days

        if not incremental:
            self.since = None
        else:
            try:
                last_extract_log = HQExtractLog.get_last_extract_log(self.__class__.__name__, d)
                self.since = last_extract_log.extract_end
            except HQExtractLog.DoesNotExist:
                self.since = None
        self._incoming_table_class = incoming_table_class
        self.engine = sqlalchemy.create_engine(conf.SQLALCHEMY_DB_URL)
        super(CommCareExportExtractor, self).__init__(self._incoming_table_class)
    
    @property
    def _get_query(self):
        pass
    
    def set_api_client(self, api_client):
        self.api_client = api_client
        
    def extract_chunk(self, since, until):
        api_call_start = datetime.datetime.now() # when did the API call start? if until is none, we can assume we fetched records up to this time
        try:
            logger.info("%s doing chunked extract for domain %s, requesting records since %s until %s" 
                        % (self.__class__.__name__, self.domain, since if since else 'forever', until if until else 'forever'))
       
            writer = PgCopyWriter(self.engine.connect(), self.api_client.project)
            
            env = BuiltInEnv() | CommCareHqEnv(self.api_client, since, until) | JsonPathEnv({})
            result = self._get_query.eval(env)
            
            if (self._get_table_name in [t['name'] for t in env.emitted_tables()]):
                with writer:
                    for table in env.emitted_tables():
                        if table['name'] == self._get_table_name:
                            writer.write_table(table, self._get_attribute_db_cols, self._get_hstore_db_col)
            else:
                logger.warn('no table emitted with name %s' % self._get_table_name)
            
            self.extract_log.extract_end = until if until else api_call_start
        
        except:
            raise
  
        finally:
            if self.extract_log.extract_end:
                self.extract_log.save()
        
    def do_extract(self):
        
        if not self.api_client:
            raise Exception('CommCareExportExtractor needs an initialized API client')
        if not self.engine:
            raise Exception('CommCareExportExtractor needs a database connection engine')
        
        self.extract_log.extract_start = self.since
        
        if self.chunked_by_date:
            until = (self.since + datetime.timedelta(days=self.chunked_by_days)) if self.since else datetime.datetime.strptime('01/01/2010', '%m/%d/%Y')
            since = self.since
            
            while until < datetime.datetime.now():
                self.extract_chunk(since, until)
                since = until
                until = since + datetime.timedelta(days=self.chunked_by_days)
           
            self.extract_chunk(since, None) # get the last chunk, until now
        
        else:
            self.extract_chunk(self.since, None)
        
        
    def do_cleanup(self):
        update_q = self._incoming_table_class.update(imported=True).where((self._incoming_table_class.domain == self.domain) 
                                                                          & ((self._incoming_table_class.imported == False) | (self._incoming_table_class.imported >> None)))
        rows = update_q.execute()
        
        logger.info('set imported = True for %d records in incoming data table %s' % (rows, self._incoming_table_class._meta.db_table))
            
class CommCareExportFormExtractor(CommCareExportExtractor):
    '''
    An extractor for forms using the CommCare Data APIs
    https://confluence.dimagi.com/display/commcarepublic/Data+APIs
    '''
    
    _incoming_table_class = IncomingForm
    
    def __init__(self, domain, chunked_by_date=True, incremental=True):
        '''
        Constructor
        '''
        try:
            self.domain_object = Domain.get(name = domain)
        except Domain.DoesNotExist:
            logger.error('No domain named %s, cannot start CommCareExportFormExtractor' % domain)
            return
        
        super(CommCareExportFormExtractor, self).__init__(self._incoming_table_class, domain, 
            chunked_by_date=chunked_by_date, incremental=incremental)
        
    @property
    def _get_formcase_query(self):
        # headings need to be lower case and not reserved words for the postgresql copy to work
        heading_list = [Literal('ccx_id'),
                              Literal('form_id'),
                              Literal('domain'),
                              Literal('case_id'),
                              Literal('alt_case_id'),
                              Literal('created'),
                              Literal('updated'),
                              Literal('closed'),
                              Literal('record_type')]
        source_field_list = [Reference('id'),
                              Reference('$.metadata.instanceID'),
                              Reference('$.domain'),
                              Reference('@case_id'),
                              Reference('case_id'),
                              Apply(Reference('bool'), Reference('create')),
                              Apply(Reference('bool'), Reference('update')),
                              Apply(Reference('bool'), Reference('close')),
                              Literal('case_event'),]
        
        if 'impact_case_properties' in self.domain_object.attributes:
            properties_list = [pr.strip() for pr in self.domain_object.attributes['impact_case_properties'].split(',')]
            heading_list.extend([Literal(prop) for prop in properties_list if prop])
            source_field_list.extend([Reference('update.%s' % prop) for prop in properties_list if prop]) 
        
        formcase_query = Emit(table=self._get_table_name,
                    headings=heading_list,
                    source=Map(source=FlatMap(body=Reference('form..case'),
                                              source=Apply(Reference('api_data'), Literal('form'))),
                               body=List(source_field_list)))
        return formcase_query
    
    @property
    def _get_form_query(self):
        # headings need to be lower case and not reserved words for the postgresql copy to work
        form_query = Emit(table=self._get_table_name,
                    headings=[Literal('ccx_id'),
                              Literal('form_id'),
                              Literal('xmlns'),
                              Literal('app_id'),
                              Literal('domain'),
                              Literal('app_version'),
                              Literal('device_id'),
                              Literal('user_id'),
                              Literal('is_phone_submission'),
                              Literal('time_start'),
                              Literal('time_end'),
                              Literal('received_on'),
                              Literal('record_type'),],
                    source=Map(source=Apply(Reference('api_data'), Literal('form')),
                               body=List([Reference('id'),
                              Reference('$.metadata.instanceID'),
                              Reference('$.form.@xmlns'),
                              Reference('$.app_id'),
                              Reference('$.domain'),
                              Reference('$.metadata.appVersion'),
                              Reference('$.metadata.deviceID'),
                              Reference('$.metadata.userID'),
                              Reference('$.is_phone_submission'),
                              Reference('$.metadata.timeStart'),
                              Reference('$.metadata.timeEnd'),
                              Reference('$.received_on'),
                              Literal('form'),])))
        return form_query
    
    def extract_chunk(self, since, until):
        api_call_start = datetime.datetime.now() # when did the API call start? if until is none, we can assume we fetched records up to this time
        try:
            logger.info("%s doing chunked extract for domain %s, requesting records since %s until %s" 
                        % (self.__class__.__name__, self.domain, since if (since 
                           and not 'impact_case_properties' in self.domain_object.attributes) 
                           else 'forever', until if until else 'forever'))
       
            params = {'limit': 1000}
            if since and not 'impact_case_properties' in self.domain_object.attributes: # if we have impact case properties pull everything
                params.update({'received_on_start':since.strftime("%Y-%m-%dT%H:%M:%S")})
            
            forms = self.api_client.iterate('form', params)
            form_data = [form for form in forms]
            mock_hq_client_data =  {'form':[({'limit': 1000},form_data)]} # doesn't work with the received_on param, don't know why

            client = MockCommCareHqClient(mock_hq_client_data)
            
            writer = PgCopyWriter(self.engine.connect(), self.api_client.project)
            
            for query in [self._get_form_query, self._get_formcase_query]:
                env = BuiltInEnv() | CommCareHqEnv(client) | JsonPathEnv({})
                result = query.eval(env)
                
                if (self._get_table_name in [t['name'] for t in env.emitted_tables()]):
                    with writer:
                        for table in env.emitted_tables():
                            writer.write_table(table, self._get_attribute_db_cols, self._get_hstore_db_col)
                else:
                    logger.warn('no table emitted with name %s' % self._get_table_name)
            
            self.extract_log.extract_end = until if until else api_call_start
        
        except:
            raise
  
        finally:
            if self.extract_log.extract_end:
                self.extract_log.save()
    
class CommCareExportCaseExtractor(CommCareExportExtractor):
    '''
    An extractor for cases using the CommCare Data APIs
    https://confluence.dimagi.com/display/commcarepublic/Data+APIs
    '''

    _incoming_table_class = IncomingCases
    
    def __init__(self, domain, chunked_by_date=True, incremental=True):
        '''
        Constructor
        '''
        super(CommCareExportCaseExtractor, self).__init__(self._incoming_table_class, domain, 
            chunked_by_date=chunked_by_date, incremental=incremental)
    
    @property
    def _get_query(self):
        case_query = Emit(table=self._get_table_name,
                   headings=[Literal('api_id'),
                             Literal('case_id'),
                             Literal('closed'),
                             Literal('date_closed'),
                             Literal('date_modified'),
                             Literal('domain'),
                             Literal('user_id'),
                             Literal('date_opened'),
                             Literal('case_type'),
                             Literal('owner_id'),
                             Literal('parent_id')],
                   source=Map(source=Apply(Reference('api_data'), Literal('case')),
                              body=List([Reference('id'),
                             Reference('case_id'),
                             Reference('closed'),
                             Reference('date_closed'),
                             Reference('date_modified'),
                             Reference('domain'),
                             Reference('user_id'),
                             Reference('properties.date_opened'),
                             Reference('properties.case_type'),
                             Reference('properties.owner_id'),
                             Reference('indices.parent.case_id')])))
        return case_query
    
class CommCareExportUserExtractor(CommCareExportExtractor):
    '''
    An extractor for mobile user data using the CommCare Data APIs
    https://confluence.dimagi.com/display/commcarepublic/Data+APIs
    '''

    _incoming_table_class = IncomingUser
    
    def __init__(self, domain, archived = False):
        '''
        Constructor
        archived = True gets archived/deactivated users only, otherwise only active users are returned
        '''  
        self.archived = archived
        super(CommCareExportUserExtractor, self).__init__(self._incoming_table_class, domain)
    
    @property
    def _get_query(self):
        user_query = Emit(table=self._get_table_name,
                   headings=[Literal('user_id'),
                             Literal('username'),
                             Literal('first_name'),
                             Literal('last_name'),
                             Literal('default_phone_number'),
                             Literal('email'),
                             Literal('groups'),
                             Literal('phone_numbers'),
                             Literal('user_data'),
                             Literal('completed_last_30'),
                             Literal('submitted_last_30'),
                             Literal('domain'),
                             Literal('deleted'),
                             Literal('deactivated')],
                   source=Map(source=Apply(Reference('api_data'), Literal('user'),
                                           Literal({"extras": True, "archived":self.archived})),
                              body=List([Reference('id'),
                             Reference('username'),
                             Reference('first_name'),
                             Reference('last_name'),
                             Reference('default_phone_number'),
                             Reference('email'),
                             Reference('groups'),
                             Reference('phone_numbers'),
                             Reference('user_data'),
                             Reference('extras.completed_last_30'),
                             Reference('extras.submitted_last_30'),
                             Literal(self.domain),
                             Literal(False), #deleted users don't show up in the API results
                             Literal(self.archived)])))
        return user_query
    
class CommCareExportWebUserExtractor(CommCareExportExtractor):
    '''
    An extractor for web user data using the CommCare Data APIs
    https://confluence.dimagi.com/display/commcarepublic/Data+APIs
    '''

    _incoming_table_class = IncomingWebUser
    
    def __init__(self, domain):
        '''
        Constructor
        '''  
        super(CommCareExportWebUserExtractor, self).__init__(self._incoming_table_class, domain)
    
    @property
    def _get_query(self):
        user_query = Emit(table=self._get_table_name,
                   headings=[Literal('api_id'),
                             Literal('username'),
                             Literal('first_name'),
                             Literal('last_name'),
                             Literal('default_phone_number'),
                             Literal('email'),
                             Literal('phone_numbers'),
                             Literal('is_admin'),
                             Literal('resource_uri'),
                             Literal('webuser_role'),
                             Literal('domain')],
                   source=Map(source=Apply(Reference('api_data'), Literal('web-user')),
                              body=List([Reference('id'),
                                         Reference('username'),
                                         Reference('first_name'),
                                         Reference('last_name'),
                                         Reference('default_phone_number'),
                                         Reference('email'),
                                         Reference('phone_numbers'),
                                         Reference('is_admin'),
                                         Reference('resource_uri'),
                                         Reference('role'),
                                         Literal(self.domain)])))
        return user_query
    
class CommCareExportDeviceLogExtractor(CommCareExportExtractor):
    '''
    An extractor for device logs using the CommCare Data APIs
    https://confluence.dimagi.com/display/commcarepublic/Data+APIs
    '''

    _incoming_table_class = IncomingDeviceLog
    
    def __init__(self, domain):
        '''
        Constructor
        '''
        super(CommCareExportDeviceLogExtractor, self).__init__(self._incoming_table_class, domain, 
            chunked_by_date=True, chunked_by_days=15)

    @property
    def _get_query(self):
        query = Emit(table=self._get_table_name,
                   headings=[Literal('app_version'),
                             Literal('log_date'),
                             Literal('device_id'),
                             Literal('domain'),
                             Literal('i'),
                             Literal('api_id'),
                             Literal('msg'),
                             Literal('resource_uri'),
                             Literal('log_type'),
                             Literal('user_id'),
                             Literal('username'),
                             Literal('xform_id')],
                   source=Map(source=Apply(Reference('api_data'), Literal('device-log')),
                              body=List([Reference('app_version'),
                             Reference('date'),
                             Reference('device_id'),
                             Literal(self.domain),
                             Reference('i'),
                             Reference('id'),
                             Reference('msg'),
                             Reference('resource_uri'),
                             Reference('type'),
                             Reference('user_id'),
                             Reference('username'),
                             Reference('xform_id'), ])))
        return query
    
class SlumberExtractor(Extractor):
    
    def __init__(self, _incoming_table_class, api_call, kwargs = None):
        self.api_call = api_call
        self.kwargs = kwargs
        super(SlumberExtractor, self).__init__(_incoming_table_class)
    
    def api_extract(self):
        url_params = self.kwargs if self.kwargs else {}
        rec_data = self.api_call.get(**url_params)
        next_page = rec_data['meta']['next']
        rec_objects = rec_data['objects']

        while next_page:
            url_params.update({'offset':rec_data['meta']['offset'] + rec_data['meta']['limit'] , 
                               'limit' : (self._limit if self._limit else rec_data['meta']['limit'])})
            rec_data = self.api_call.get(**url_params)
            next_page = rec_data['meta']['next']
            rec_objects.extend(rec_data['objects'])

        return rec_objects
    
    def do_extract(self):
        rec_objects = self.api_extract()
        self.save_incoming(rec_objects)
        
    def save_incoming(self, rec_objects):
        raise NotImplementedError('Subclass must implement save_incoming')
    
    def do_cleanup(self):
        update_q = self._incoming_table_class.update(imported=True).where((self._incoming_table_class.imported == False) | 
                                                                          (self._incoming_table_class.imported >> None))
        rows = update_q.execute()
        logger.info('set imported = True for %d records in incoming data table %s' % (rows, self._incoming_table_class._meta.db_table))
      

class CommCareSlumberFormDefExtractor(SlumberExtractor):
    '''
    An extractor for application structure data using the CommCare Data APIs
    https://confluence.dimagi.com/display/commcarepublic/Data+APIs
    This one doesn't use commcare-export. I couldn't figure out the jsonpath.
    '''
    
    # actually both IncomingApplication and IncomingFormDef - legacy
    _incoming_table_class = IncomingFormDef
    
    def __init__(self, api_version, domain, username, password):
        '''
        Constructor
        '''
        self.domain=domain
        url = "https://www.commcarehq.org/a/%s/api/%s/" % (domain,api_version)
        api = slumber.API(url, auth=HTTPDigestAuth(username, password))
        kwargs = {'extras':True}
        api_call = getattr(api, 'application')
        super(CommCareSlumberFormDefExtractor, self).__init__(self._incoming_table_class, api_call, kwargs)
        
    def save_incoming(self, app_objects):
        for app in app_objects:
            module_data = app['modules']
            inc_app = IncomingApplication.create(app_id=app["id"], app_name=app["name"],attributes_json=app, domain=self.domain)
            inc_app.save()
            
            for mod in module_data:
                if 'forms' in mod:
                    for form in mod['forms']:
                        names = json.dumps(form['name'])
                        fd = IncomingFormDef(app_id=app["id"], app_name=app["name"], 
                                             domain=self.domain, case_type=mod["case_type"],form_names=names, form_xmlns = form["xmlns"])
                        fd.formdef_json = json.dumps(form, ensure_ascii=False)
                        fd.save()
        
    def do_cleanup(self):
        update_q = self._incoming_table_class.update(imported=True).where((self._incoming_table_class.domain == self.domain) 
                                                                          & ((self._incoming_table_class.imported == False) 
                                                                             | (self._incoming_table_class.imported >> None)))
        rows = update_q.execute()
        logger.info('set imported = True for %d records in incoming data table %s' % (rows, self._incoming_table_class._meta.db_table))
        
        # also update IncomingApplication, this loader loads both formdefs and apps
        inc_app_update_q = IncomingApplication.update(imported=True).where((IncomingApplication.domain == self.domain) 
                                                                          & ((IncomingApplication.imported == False) 
                                                                             | (IncomingApplication.imported >> None)))
        rows = inc_app_update_q.execute()
        logger.info('set imported = True for %d records in incoming data table %s' % (rows, IncomingApplication._meta.db_table))

class HQAdminAPIExtractor(SlumberExtractor):
    
    base_url = 'https://www.commcarehq.org/hq/admin/api/global/'
    
    def __init__(self, username, password):
        
        api = slumber.API(self.base_url, auth=HTTPDigestAuth(username, password))
        
        if not self._api_endpoint:
            raise NotImplementedError("Subclass must specify api endpoint")
        
        if not self._incoming_table_class:
            raise NotImplementedError("Subclass must specify incoming table class")
        
        if self._api_endpoint in ('project_space_metadata', 'web-user'):
            api_call = getattr(api, self._api_endpoint)
        else:
            raise NotImplementedError("Don't know how to fetch %s from API" % self._api_endpoint)
        
        super(HQAdminAPIExtractor, self).__init__(self._incoming_table_class, api_call)
    

class WebuserAdminAPIExtractor(HQAdminAPIExtractor):
    
    _incoming_table_class = IncomingWebUser
    _api_endpoint = 'web-user'
    _limit = 100
    
    def __init__(self, username, password):
        
        super(WebuserAdminAPIExtractor, self).__init__(username, password)
        
    def save_incoming(self, rec_objects):
        inc_web_users = []
        for obj in rec_objects:
            obj['api_id'] = obj['id']
            obj.pop('id', None) # don't keep the ID in or it will try to set the autogenerated db id
            obj['phone_numbers'] = ','.join(obj['phone_numbers'])
            
            inc_domains = obj['domains']
            obj.pop('domains', None) 
            if not inc_domains:
                obj['domain'] = None
                inc_web_users.append(obj)
            else:
                
                for domain in inc_domains:
                    obj.update({'domain':domain})
                    inc_web_users.append(obj)
        
        deduped = [dict(t) for t in set([tuple(d.items()) for d in inc_web_users])]
        self._incoming_table_class.insert_many(deduped).execute()
                    
class ProjectSpaceAdminAPIExtractor(HQAdminAPIExtractor):
    
    _incoming_table_class = IncomingWebUser
    _api_endpoint = 'project_space_metadata'
    _limit = 100
    
    def __init__(self, username, password):
        
        super(ProjectSpaceAdminAPIExtractor, self).__init__(username, password)
        
    def save_incoming(self, rec_objects):
        for obj in rec_objects:
            IncomingDomain.create(api_json=obj)
    
class ExcelExtractor(Extractor):
    '''
    An extractor for excel files. 
    One sheet only for now. 
    Expects column names in first row, rest of rows mapped 1:1 to incoming table rows.
    Unique identifier (or unique for domain) in first column.
    '''
    
    def __init__(self, incoming_table_class, file_name):
        '''
        Constructor
        '''
        self._incoming_table_class = incoming_table_class
        self.file_name = file_name
        
        self.workbook = ExcelFile(os.path.join(conf.INPUT_DIR, file_name))
        
        super(ExcelExtractor, self).__init__(self._incoming_table_class)
        
    def _get_workbook_rowdicts(self):
        '''
        returns list of key-value dicts for all rows in sheet, with keys in first row. empty values are removed.
        '''
        rows = self.workbook.parse().to_dict(outtype='records')
        rows_ret = list()
        for row in rows:
            ret = dict((k, v) for k, v in row.iteritems() if notnull(v))
            rows_ret.append(ret)
        return rows_ret
        
    def _get_workbook_keys(self):
        '''
        returns list of key-value dicts from keys in first row
        '''
        return self.workbook.parse().to_dict().keys()
        
    
    def do_extract(self):
        
        db_col_keys = [k for k in self._get_workbook_keys() if k in self._get_db_cols]
        hstore_keys = [h for h in self._get_workbook_keys() if h not in self._get_db_cols]
        
        for row in self._get_workbook_rowdicts():
            db_col_dict = dict((k, v) for k, v in row.iteritems() if k in db_col_keys)
            hstore_col_dict = dict((k, unicode(v)) for k, v in row.iteritems() if k in hstore_keys)
            
            insert_dict = db_col_dict
            insert_dict[self._get_hstore_db_col] = hstore_col_dict

            self._incoming_table_class.create(**insert_dict)
            
    def do_cleanup(self):
        delete_q = self._incoming_table_class.delete()
        rows = delete_q.execute()
        logger.info('Deleted %d records in incoming data table %s' % (rows, self._incoming_table_class._meta.db_table))
        
class SalesforceExtractor(Extractor):
    
    _incoming_table_class = IncomingSalesforceRecord
    
    def __init__(self, username, password, token):
        '''
        Constructor
        '''
        self.api = Salesforce(username=username, password=password, security_token=token)
        super(SalesforceExtractor, self).__init__(self._incoming_table_class)
    
    def do_extract(self):
        res = self.api.describe()
        obj_names = [obj['name'] for obj in res['sobjects']]
        obj_fields = dict()
        
        for obj in obj_names:
            api_method_call =  getattr(self.api, obj)
            try:
                obj_meta = api_method_call.describe()
                # get field names for everything except the content blobs
                # if there's a blob in the record queries will return only one at a time i.e. one API call per record
                field_names = [field['name'] for field in obj_meta['fields'] if field['type'] not in ('base64')]
                obj_fields[obj] = field_names
                
            except SalesforceMalformedRequest, m:
                logger.warn('Got SalesforceMalformedRequest when querying metadata for object %s' % obj)
                logger.warn(str(m))
                
        for obj in obj_fields.keys():
            fieldstr = ','.join(obj_fields[obj])
            querystr = 'SELECT %s FROM %s' % (fieldstr,obj)
            logger.info('Executing Salesforce query %s' % querystr)
            records = []
            try:
                obj_res = self.api.query(querystr)
                if 'records' in obj_res:
                    records.extend(obj_res['records'])
                while 'done' in obj_res and not obj_res['done']:
                    obj_res = self.api.query_more(obj_res['nextRecordsUrl'], True)
                    if 'records' in obj_res:
                        records.extend(obj_res['records'])
                
                logger.info('%d results of type %s' % (len(records),obj))
                for rec in records:
                    IncomingSalesforceRecord.create(sf_id=id,object_type=obj,record=json.dumps(rec))
                    
            except SalesforceMalformedRequest, m:
                logger.warn('Got SalesforceMalformedRequest when trying to retrieve all fields for object %s' % obj)
                logger.warn(str(m))

    
    def do_cleanup(self):
        delete_q = self._incoming_table_class.delete()
        rows = delete_q.execute()
        logger.info('Deleted %d records in incoming data table %s' % (rows, self._incoming_table_class._meta.db_table))
    
