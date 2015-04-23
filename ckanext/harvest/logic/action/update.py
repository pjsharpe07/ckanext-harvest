import hashlib

import logging
import datetime
import json

from pylons import config
from paste.deploy.converters import asbool
from sqlalchemy import and_

from ckan.lib.search.index import PackageSearchIndex
from ckan.plugins import PluginImplementations
from ckan.logic import get_action
from ckanext.harvest.interfaces import IHarvester
from ckan.lib.search.common import SearchIndexError, make_connection


from ckan.model import Package
from ckan import logic

from ckan.logic import NotFound, check_access

from ckanext.harvest.plugin import DATASET_TYPE_NAME
from ckanext.harvest.queue import get_gather_publisher, resubmit_jobs

from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject
from ckanext.harvest.logic import HarvestJobExists
from ckanext.harvest.logic.schema import harvest_source_show_package_schema

from ckanext.harvest.logic.action.get import harvest_source_show, harvest_job_list, _get_sources_for_user
import ckan.lib.mailer as mailer 

from ckanext.harvest.logic.dictization import harvest_job_dictize

log = logging.getLogger(__name__)

def harvest_source_update(context,data_dict):
    '''
    Updates an existing harvest source

    This method just proxies the request to package_update,
    which will create a harvest_source dataset type and the
    HarvestSource object. All auth checks and validation will
    be done there .We only make sure to set the dataset type

    Note that the harvest source type (ckan, waf, csw, etc)
    is now set via the source_type field.

    :param id: the name or id of the harvest source to update
    :type id: string
    :param url: the URL for the harvest source
    :type url: string
    :param name: the name of the new harvest source, must be between 2 and 100
        characters long and contain only lowercase alphanumeric characters
    :type name: string
    :param title: the title of the dataset (optional, default: same as
        ``name``)
    :type title: string
    :param notes: a description of the harvest source (optional)
    :type notes: string
    :param source_type: the harvester type for this source. This must be one
        of the registerd harvesters, eg 'ckan', 'csw', etc.
    :type source_type: string
    :param frequency: the frequency in wich this harvester should run. See
        ``ckanext.harvest.model`` source for possible values. Default is
        'MANUAL'
    :type frequency: string
    :param config: extra configuration options for the particular harvester
        type. Should be a serialized as JSON. (optional)
    :type config: string


    :returns: the newly created harvest source
    :rtype: dictionary

    '''
    log.info('Updating harvest source: %r', data_dict)

    data_dict['type'] = DATASET_TYPE_NAME

    context['extras_as_string'] = True
    package_dict = logic.get_action('package_update')(context, data_dict)

    context['schema'] = harvest_source_show_package_schema()
    source = logic.get_action('package_show')(context, package_dict)

    return source

def harvest_source_clear(context,data_dict):
    '''
    Clears all datasets, jobs and objects related to a harvest source, but keeps the source itself.
    This is useful to clean history of long running harvest sources to start again fresh.

    :param id: the id of the harvest source to clear
    :type id: string

    '''
    check_access('harvest_source_clear',context,data_dict)

    harvest_source_id = data_dict.get('id',None)

    source = HarvestSource.get(harvest_source_id)
    if not source:
        log.error('Harvest source %s does not exist', harvest_source_id)
        raise NotFound('Harvest source %s does not exist' % harvest_source_id)

    harvest_source_id = source.id

    # Clear all datasets from this source from the index
    harvest_source_index_clear(context, data_dict)


    sql = '''begin; update package set state = 'to_delete' where id in (select package_id from harvest_object where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object_error where harvest_object_id in (select id from harvest_object where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object_extra where harvest_object_id in (select id from harvest_object where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object where harvest_source_id = '{harvest_source_id}';
    delete from harvest_gather_error where harvest_job_id in (select id from harvest_job where source_id = '{harvest_source_id}');
    delete from harvest_job where source_id = '{harvest_source_id}';
    delete from package_role where package_id in (select id from package where state = 'to_delete' );
    delete from user_object_role where id not in (select user_object_role_id from package_role) and context = 'Package';
    delete from resource_revision where resource_group_id in (select id from resource_group where package_id in (select id from package where state = 'to_delete'));
    delete from resource_group_revision where package_id in (select id from package where state = 'to_delete');
    delete from package_tag_revision where package_id in (select id from package where state = 'to_delete');
    delete from member_revision where table_id in (select id from package where state = 'to_delete');
    delete from package_extra_revision where package_id in (select id from package where state = 'to_delete');
    delete from package_revision where id in (select id from package where state = 'to_delete');
    delete from package_tag where package_id in (select id from package where state = 'to_delete');
    delete from resource where resource_group_id in (select id from resource_group where package_id in (select id from package where state = 'to_delete'));
    delete from package_extra where package_id in (select id from package where state = 'to_delete');
    delete from member where table_id in (select id from package where state = 'to_delete');
    delete from resource_group where package_id  in (select id from package where state = 'to_delete');
    delete from package where id in (select id from package where state = 'to_delete'); commit;'''.format(harvest_source_id=harvest_source_id)

    model = context['model']

    model.Session.execute(sql)

    # Refresh the index for this source to update the status object
    context.update({'validate': False, 'ignore_auth': True})
    package_dict = logic.get_action('package_show')(context,
            {'id': harvest_source_id})

    if package_dict:
        package_index = PackageSearchIndex()
        package_index.index_package(package_dict)

    return {'id': harvest_source_id}

def harvest_source_index_clear(context,data_dict):

    check_access('harvest_source_clear',context,data_dict)
    harvest_source_id = data_dict.get('id',None)

    source = HarvestSource.get(harvest_source_id)
    if not source:
        log.error('Harvest source %s does not exist', harvest_source_id)
        raise NotFound('Harvest source %s does not exist' % harvest_source_id)

    harvest_source_id = source.id

    conn = make_connection()
    query = ''' +%s:%s +site_id:"%s" ''' % ('harvest_source_id', harvest_source_id,
                                            config.get('ckan.site_id'))
    try:
        conn.delete_query(query)
        if asbool(config.get('ckan.search.solr_commit', 'true')):
            conn.commit()
    except Exception, e:
        log.exception(e)
        raise SearchIndexError(e)
    finally:
        conn.close()

    return {'id': harvest_source_id}

def harvest_objects_import(context,data_dict):
    '''
        Reimports the current harvest objects
        It performs the import stage with the last fetched objects, optionally
        belonging to a certain source.
        Please note that no objects will be fetched from the remote server.
        It will only affect the last fetched objects already present in the
        database.
    '''
    log.info('Harvest objects import: %r', data_dict)
    check_access('harvest_objects_import',context,data_dict)

    model = context['model']
    session = context['session']
    source_id = data_dict.get('source_id',None)

    segments = context.get('segments',None)

    join_datasets = context.get('join_datasets',True)

    if source_id:
        source = HarvestSource.get(source_id)
        if not source:
            log.error('Harvest source %s does not exist', source_id)
            raise NotFound('Harvest source %s does not exist' % source_id)

        if not source.active:
            log.warn('Harvest source %s is not active.', source_id)
            raise Exception('This harvest source is not active')

        last_objects_ids = session.query(HarvestObject.id) \
                .join(HarvestSource) \
                .filter(HarvestObject.source==source) \
                .filter(HarvestObject.current==True)

    else:
        last_objects_ids = session.query(HarvestObject.id) \
                .filter(HarvestObject.current==True) \

    if join_datasets:
        last_objects_ids = last_objects_ids.join(Package) \
            .filter(Package.state==u'active')

    last_objects_ids = last_objects_ids.all()

    last_objects_count = 0

    for obj_id in last_objects_ids:
        if segments and str(hashlib.md5(obj_id[0]).hexdigest())[0] not in segments:
            continue

        obj = session.query(HarvestObject).get(obj_id)

        for harvester in PluginImplementations(IHarvester):
            if harvester.info()['name'] == obj.source.type:
                if hasattr(harvester,'force_import'):
                    harvester.force_import = True
                harvester.import_stage(obj)
                break
        last_objects_count += 1
    log.info('Harvest objects imported: %s', last_objects_count)
    return last_objects_count

def _caluclate_next_run(frequency):

    now = datetime.datetime.utcnow()
    if frequency == 'ALWAYS':
        return now
    if frequency == 'WEEKLY':
        return now + datetime.timedelta(weeks=1)
    if frequency == 'BIWEEKLY':
        return now + datetime.timedelta(weeks=2)
    if frequency == 'DAILY':
        return now + datetime.timedelta(days=1)
    if frequency == 'MONTHLY':
        if now.month in (4,6,9,11):
            days = 30
        elif now.month == 2:
            if now.year % 4 == 0:
                days = 29
            else:
                days = 28
        else:
            days = 31
        return now + datetime.timedelta(days=days)
    raise Exception('Frequency {freq} not recognised'.format(freq=frequency))


def _make_scheduled_jobs(context, data_dict):

    data_dict = {'only_to_run': True,
                 'only_active': True}
    sources = _get_sources_for_user(context, data_dict)

    for source in sources:
        data_dict = {'source_id': source.id}
        try:
            get_action('harvest_job_create')(context, data_dict)
        except HarvestJobExists, e:
            log.info('Trying to rerun job for %s skipping' % source.id)

        source.next_run = _caluclate_next_run(source.frequency)
        source.save()

def harvest_jobs_run(context,data_dict):
    log.info('Harvest job run: %r', data_dict)
    check_access('harvest_jobs_run',context,data_dict)

    model = context['model']
    session = context['session']

    source_id = data_dict.get('source_id',None)

    if not source_id:
        _make_scheduled_jobs(context, data_dict)

    context['return_objects'] = False

    # Flag finished jobs as such
    jobs = harvest_job_list(context,{'source_id':source_id,'status':u'Running'})
    if len(jobs):
        package_index = PackageSearchIndex()        
        for job in jobs:            
            if job['gather_finished']:                
                objects = session.query(HarvestObject.id) \
                          .filter(HarvestObject.harvest_job_id==job['id']) \
                          .filter(and_((HarvestObject.state!=u'COMPLETE'),
                                       (HarvestObject.state!=u'ERROR'))) \
                          .order_by(HarvestObject.import_finished.desc())

                if objects.count() == 0:                    
                    job_obj = HarvestJob.get(job['id'])
                    job_obj.status = u'Finished'

                    last_object = session.query(HarvestObject) \
                          .filter(HarvestObject.harvest_job_id==job['id']) \
                          .filter(HarvestObject.import_finished!=None) \
                          .order_by(HarvestObject.import_finished.desc()) \
                          .first()
                    if last_object:                        
                        job_obj.finished = last_object.import_finished
                    else:
                        job_obj.finished = datetime.datetime.utcnow()
                    job_obj.save()                   

                    # recreate job for datajson collection or the like.
                    source = job_obj.source
                    source_config = json.loads(source.config or '{}')
                    datajson_collection = source_config.get(
                        'datajson_collection')
                    if datajson_collection == 'parents_run':
                        new_job = HarvestJob()
                        new_job.source = source
                        new_job.save()
                        source_config['datajson_collection'] = 'children_run'
                        source.config = json.dumps(source_config)
                        source.save()
                    elif datajson_collection:
                        # reset the key if 'children_run', or anything.
                        source_config.pop("datajson_collection", None)
                        source.config = json.dumps(source_config)
                        source.save()

                    if config.get('ckanext.harvest.email', 'on') == 'on':
                      #email body

                      sql = '''select name from package where id = :source_id;'''

                      q = model.Session.execute(sql, {'source_id' : job_obj.source_id})

                      for row in q:
                        harvest_name = str(row['name'])

                      job_url = config.get('ckan.site_url') + '/harvest/' + harvest_name + '/job/' + job_obj.id

                      msg = 'Local Here is the summary of latest harvest job (' + job_url + ') set-up for your organization in Data.gov\n\n'
                    
                      sql = '''select g.title as org, s.title as job_title from member m
                               join public.group g on m.group_id = g.id
                               join harvest_source s on s.id = m.table_id
                               where table_id = :source_id;'''
                             
                      q = model.Session.execute(sql, {'source_id' : job_obj.source_id})
                    
                      for row in q:
                          msg += 'Organization: ' + str(row['org']) + '\n\n'
                          msg += 'Harvest Job Title: ' + str(row['job_title']) + '\n\n'
    
                      msg += 'Date of Harvest: ' + str(job_obj.created) + ' GMT\n\n'

                      out = {
                          'last_job': None,
                      }
                    
                      out['last_job'] = harvest_job_dictize(job_obj, context)
                    
                      msg += 'Records in Error: ' + str(out['last_job']['stats'].get('errored',0)) + '\n'
                      msg += 'Records Added: ' + str(out['last_job']['stats'].get('added',0)) + '\n'
                      msg += 'Records Updated: ' + str(out['last_job']['stats'].get('updated',0)) + '\n'
                      msg += 'Records Deleted: ' + str(out['last_job']['stats'].get('deleted',0)) + '\n\n'
       
                      obj_error = ''
                      job_error = ''
                      all_updates = ''

                      sql = '''select hoe.message as msg, ho.package_id as package_id from harvest_object ho
                              inner join harvest_object_error hoe on hoe.harvest_object_id = ho.id
                              where ho.harvest_job_id = :job_id;'''
                      q = model.Session.execute(sql, {'job_id' : job_obj.id})
                      for row in q:
                         obj_error += row['msg'] + '\n'

                      #get all packages added and updated by harvest job
                      sql = '''select ho.package_id as ho_package_id, ho.harvest_source_id, ho.report_status as ho_package_status, package.title as package_title
                               from harvest_object ho
                               inner join package on package.id = ho.package_id
                               where ho.harvest_job_id = :job_id and (ho.report_status = 'added' or ho.report_status = 'updated')
                               order by ho.report_status ASC;'''

                      q = model.Session.execute(sql, {'job_id': job_obj.id})
                      for row in q:
                         if row['ho_package_status'] == 'added':
                            all_updates += row['ho_package_status'].upper() + '    , ' + row['ho_package_id'] + ', ' + row['package_title'] + '\n'
                         else:
                            all_updates += row['ho_package_status'].upper() + ' , ' + row['ho_package_id'] + ', ' + row['package_title'] + '\n'


                      #get all packages deleted by harvest job
                      sql = '''SELECT ho.harvest_job_id, ho.harvest_source_id, ho.package_id, ho.report_status, package.title, ho.guid
                               FROM harvest_object ho
                               inner join package on package.id = ho.guid
                               where harvest_job_id = :job_id and ho.report_status = 'deleted'
                               order by report_status ASC;'''

                      q = model.Session.execute(sql, {'job_id': job_obj.id})
                      for row in q:
                         all_updates += row['ho_package_status'].upper() + ', ' + row['ho_package_id'] + ', ' + row['package_title'] + '\n'

                      if(all_updates != ''):
                        msg += 'Summary\n\n' + all_updates + '\n\n'

                      sql = '''select message from harvest_gather_error where harvest_job_id = :job_id; '''
                      q = model.Session.execute(sql, {'job_id' : job_obj.id})
                      for row in q:
                        job_error += row['message'] + '\n'
                  
                      if(obj_error != '' or job_error != ''):
                        msg += 'Error Summary\n\n'
                    
                      if(obj_error != ''):
                        msg += 'Document Error\n' + obj_error + '\n\n'
                    
                      if(job_error != ''):
                        msg += 'Job Errors\n' + job_error + '\n\n'

                      msg += '\n--\nYou are receiving this email because you are currently set-up as Administrator for your organization in Data.gov. Please do not reply to this email as it was sent from a non-monitored address. Please feel free to contact us at www.data.gov/contact for any questions or feedback.'

                      #get recipients
                      sql = '''select group_id from member where table_id = :source_id;'''
                      q = model.Session.execute(sql, {'source_id' : job_obj.source_id})
                  
                      for row in q:
                          sql = '''select email, name from public.user u
                                  join member m on m.table_id = u.id
                                  where capacity = 'admin' and state = 'active' and group_id = :group_id;'''

                          q1 = model.Session.execute(sql, {'group_id' : row['group_id']})
                    
                          for row1 in q1:
                              email = {'recipient_name': str(row1['name']),
                                       'recipient_email': str(row1['email']),
                                       'subject': 'Local Data.gov Latest Harvest Job Report',
                                       'body': msg}
         
                              try:
                                  mailer.mail_recipient(**email)
                              except Exception:
                                  pass
 
                    # Reindex the harvest source dataset so it has the latest
                    # status
                    if 'extras_as_string'in context:
                        del context['extras_as_string']
                    context.update({'validate': False, 'ignore_auth': True})
                    package_dict = logic.get_action('package_show')(context,
                            {'id': job_obj.source.id})

                    if package_dict:
                        package_index.index_package(package_dict)
            
    # resubmit old redis tasks
    resubmit_jobs()

    # Check if there are pending harvest jobs
    jobs = harvest_job_list(context,{'source_id':source_id,'status':u'New'})
    if len(jobs) == 0:
        log.info('No new harvest jobs.')
    
    # Send each job to the gather queue
    publisher = get_gather_publisher()
    sent_jobs = []
    for job in jobs:
        context['detailed'] = False     
        source = harvest_source_show(context,{'id':job['source_id']})
        #source = harvest_source_show(context,{'id':source_id})
        if source['active']:
            job_obj = HarvestJob.get(job['id'])
            job_obj.status = job['status'] = u'Running'
            job_obj.save()
            publisher.send({'harvest_job_id': job['id']})
            log.info('Sent job %s to the gather queue' % job['id'])
            sent_jobs.append(job)

    publisher.close()
    return sent_jobs

def harvest_sources_reindex(context, data_dict):
    '''
        Reindexes all harvest source datasets with the latest status
    '''
    log.info('Reindexing all harvest sources')
    check_access('harvest_sources_reindex', context, data_dict)

    model = context['model']

    packages = model.Session.query(model.Package) \
                            .filter(model.Package.type==DATASET_TYPE_NAME) \
                            .filter(model.Package.state==u'active') \
                            .all()

    package_index = PackageSearchIndex()
    for package in packages:
        if 'extras_as_string'in context:
            del context['extras_as_string']
        context.update({'validate': False, 'ignore_auth': True})
        package_dict = logic.get_action('package_show')(context,
            {'id': package.id})
        log.debug('Updating search index for harvest source {0}'.format(package.id))
        package_index.index_package(package_dict, defer_commit=True)

    package_index.commit()
    log.info('Updated search index for {0} harvest sources'.format(len(packages)))
