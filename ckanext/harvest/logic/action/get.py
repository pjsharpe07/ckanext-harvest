import logging
from sqlalchemy import or_
from ckan.model import User
import datetime

from ckan import logic
from ckan.plugins import PluginImplementations
from ckanext.harvest.interfaces import IHarvester

import ckan.plugins as p
from ckan.logic import NotFound, check_access, side_effect_free

from ckanext.harvest import model as harvest_model

from ckanext.harvest.model import (HarvestSource, HarvestJob, HarvestObject)
from ckanext.harvest.logic.dictization import (harvest_source_dictize,
                                               harvest_job_dictize,
                                               harvest_object_dictize)
                                               
import ckan.lib.mailer as mailer                                              

log = logging.getLogger(__name__)

@side_effect_free
def harvest_source_show(context,data_dict):
    '''
    Returns the metadata of a harvest source

    This method just proxies the request to package_show. All auth checks and
    validation will be done there.

    :param id: the id or name of the harvest source
    :type id: string

    :returns: harvest source metadata
    :rtype: dictionary
    '''


    source_dict = logic.get_action('package_show')(context, data_dict)

    # For compatibility with old code, add the active field
    # based on the package state
    source_dict['active'] = (source_dict['state'] == 'active')

    return source_dict

@side_effect_free
def harvest_source_show_status(context, data_dict):
    '''
    Returns a status report for a harvest source

    Given a particular source, returns a dictionary containing information
    about the source jobs, datasets created, errors, etc.
    Note that this information is already included on the output of
    harvest_source_show, under the 'status' field.

    :param id: the id or name of the harvest source
    :type id: string

    :rtype: dictionary
    '''

    p.toolkit.check_access('harvest_source_show_status', context, data_dict)


    model = context.get('model')

    source = harvest_model.HarvestSource.get(data_dict['id'])
    if not source:
        raise p.toolkit.ObjectNotFound('Harvest source {0} does not exist'.format(data_dict['id']))

    out = {
           'job_count': 0,
           'last_job': None,
           'total_datasets': 0,
           }

    jobs = harvest_model.HarvestJob.filter(source=source).all()

    job_count = len(jobs)
    if job_count == 0:
        return out

    out['job_count'] = job_count

    # Get the most recent job
    last_job = harvest_model.HarvestJob.filter(source=source) \
               .order_by(harvest_model.HarvestJob.created.desc()).first()

    if not last_job:
        return out

    out['last_job'] = harvest_job_dictize(last_job, context)

    # Overall statistics
    packages = model.Session.query(model.Package) \
            .join(harvest_model.HarvestObject) \
            .filter(harvest_model.HarvestObject.harvest_source_id==source.id) \
            .filter(harvest_model.HarvestObject.current==True) \
            .filter(model.Package.state==u'active') \
            .filter(model.Package.private==False) \
            .group_by(model.Package.id)
    out['total_datasets'] = packages.count()

    if(last_job.status == 'Finished'):     
      
      #email body
      msg = 'Here is the summary of latest harvest job set-up for your organization in Data.gov\n\n'
      
      sql = '''select g.title as org, s.title as job_title from member m
               join public.group g on m.group_id = g.id
               join harvest_source s on s.id = m.table_id
               where table_id = :source_id;'''
      q = model.Session.execute(sql, {'source_id' : last_job.source_id})
      
      for row in q:
        msg += 'Organization: ' + str(row['org']) + '\n\n'
        msg += 'Harvest Job Title: ' + str(row['job_title']) + '\n\n'
        
      msg += 'Date of Harvest: ' + str(last_job.created) + '\n\n'
             
      msg += 'Records in Error: ' + str(out['last_job']['stats'].get('errored',0)) + '\n' 
      msg += 'Records Added: ' + str(out['last_job']['stats'].get('added',0)) + '\n' 
      msg += 'Records Updated: ' + str(out['last_job']['stats'].get('updated',0)) + '\n' 
      msg += 'Records Deleted: ' + str(out['last_job']['stats'].get('deleted',0)) + '\n\n'
           
      obj_error = ''
      job_error = ''
      sql = '''select hoe.message as msg from harvest_object ho 
               inner join harvest_object_error hoe on hoe.harvest_object_id = ho.id
               where ho.harvest_job_id = :job_id;'''
               
      q = model.Session.execute(sql, {'job_id' : last_job.id})
      for row in q:	
        obj_error += row['msg'] + '\n'     
      
      sql = '''select message from harvest_gather_error where harvest_job_id = :job_id; '''  
      q = model.Session.execute(sql, {'job_id' : last_job.id})
      for row in q:	
        job_error += row['message'] + '\n'
      
      if(obj_error != '' or job_error != ''):
        msg += 'Error Summary\n\n'
        
      if(obj_error != ''):
        msg += 'Document Error\n' + obj_error + '\n\n'
        
      if(job_error != ''):
        msg += 'Job Errors\n' + job_error + '\n\n'
      
      msg += '\n--\nYou are receiving this email because you are currently set-up as Administrator for your organization in Data.gov. Please do not reply to this email as it was sent from a non-monitored address. Please feel free to contact us here <www.data.gov/contact> for any questions or feedback.'

      #get recipients
      sql = '''select group_id from member where table_id = :source_id;'''
      q = model.Session.execute(sql, {'source_id' : last_job.source_id})
      
      for row in q:	
        sql = '''select email, name from public.user u
                 join member m on m.table_id = u.id
                 where capacity = 'admin' and group_id = :group_id;'''
        q1 = model.Session.execute(sql, {'group_id' : row['group_id']})         
        
        for row1 in q1:        
            email = {'recipient_name': str(row1['name']),
                  'recipient_email': str(row1['email']),
                  'subject': 'Data.gov Latest Harvest Job Report', 
                  'body': msg}

            mailer.mail_recipient(**email) 
            
    return out

@side_effect_free
def harvest_source_list(context, data_dict):
    '''
    TODO: Use package search
    '''

    check_access('harvest_source_list',context,data_dict)

    model = context['model']
    session = context['session']
    user = context.get('user','')

    sources = _get_sources_for_user(context, data_dict)

    context.update({'detailed':False})
    return [harvest_source_dictize(source, context) for source in sources]

@side_effect_free
def harvest_source_for_a_dataset(context, data_dict):
    '''
    TODO: Deprecated, harvest source id is added as an extra to each dataset
    automatically
    '''
    '''For a given dataset, return the harvest source that
    created or last updated it, otherwise NotFound.'''

    model = context['model']
    session = context['session']

    dataset_id = data_dict.get('id')

    query = session.query(HarvestSource)\
            .join(HarvestObject)\
            .filter_by(package_id=dataset_id)\
            .order_by(HarvestObject.gathered.desc())
    source = query.first() # newest

    if not source:
        raise NotFound

    return harvest_source_dictize(source,context)

@side_effect_free
def harvest_job_show(context,data_dict):

    check_access('harvest_job_show',context,data_dict)

    id = data_dict.get('id')
    attr = data_dict.get('attr',None)

    job = HarvestJob.get(id,attr=attr)
    if not job:
        raise NotFound

    return harvest_job_dictize(job,context)

@side_effect_free
def harvest_job_report(context, data_dict):

    check_access('harvest_job_show', context, data_dict)

    model = context['model']
    id = data_dict.get('id')

    job = HarvestJob.get(id)
    if not job:
        raise NotFound

    report = {
        'gather_errors': [],
        'object_errors': {}
    }

    # Gather errors
    q = model.Session.query(harvest_model.HarvestGatherError) \
                      .join(harvest_model.HarvestJob) \
                      .filter(harvest_model.HarvestGatherError.harvest_job_id==job.id) \
                      .order_by(harvest_model.HarvestGatherError.created.desc())

    for error in q.all():
        report['gather_errors'].append({
            'message': error.message
        })

    # Object errors

    # Check if the harvester for this job's source has a method for returning
    # the URL to the original document
    original_url_builder = None
    for harvester in PluginImplementations(IHarvester):
        if harvester.info()['name'] == job.source.type:
             if hasattr(harvester, 'get_original_url'):
                original_url_builder = harvester.get_original_url

    q = model.Session.query(harvest_model.HarvestObjectError, harvest_model.HarvestObject.guid) \
                      .join(harvest_model.HarvestObject) \
                      .filter(harvest_model.HarvestObject.harvest_job_id==job.id) \
                      .order_by(harvest_model.HarvestObjectError.harvest_object_id)

    for error, guid in q.all():
        if not error.harvest_object_id in report['object_errors']:
            report['object_errors'][error.harvest_object_id] = {
                'guid': guid,
                'errors': []
            }
            if original_url_builder:
                url = original_url_builder(error.harvest_object_id)
                if url:
                    report['object_errors'][error.harvest_object_id]['original_url'] = url

        report['object_errors'][error.harvest_object_id]['errors'].append({
            'message': error.message,
            'line': error.line,
            'type': error.stage
         })

    return report

@side_effect_free
def harvest_job_list(context,data_dict):

    check_access('harvest_job_list',context,data_dict)

    model = context['model']
    session = context['session']

    source_id = data_dict.get('source_id',False)
    status = data_dict.get('status', False)

    query = session.query(HarvestJob)

    if source_id:
        query = query.filter(HarvestJob.source_id==source_id)

    if status:
        query = query.filter(HarvestJob.status==status)

    query = query.order_by(HarvestJob.created.desc())

    jobs = query.all()

    context['return_error_summary'] = False
    return [harvest_job_dictize(job, context) for job in jobs]

@side_effect_free
def harvest_object_show(context,data_dict):

    p.toolkit.check_access('harvest_object_show', context, data_dict)

    id = data_dict.get('id')
    dataset_id = data_dict.get('dataset_id')

    if id:
        attr = data_dict.get('attr',None)
        obj = HarvestObject.get(id,attr=attr)
    elif dataset_id:
        model = context['model']

        pkg = model.Package.get(dataset_id)
        if not pkg:
            raise p.toolkit.ObjectNotFound('Dataset not found')

        obj = model.Session.query(HarvestObject) \
              .filter(HarvestObject.package_id == pkg.id) \
              .filter(HarvestObject.current == True) \
              .first()
    else:
        raise p.toolkit.ValidationError(
            'Please provide either an "id" or a "dataset_id" parameter')

    if not obj:
        raise p.toolkit.ObjectNotFound('Harvest object not found')


    return harvest_object_dictize(obj, context)

@side_effect_free
def harvest_object_list(context,data_dict):

    check_access('harvest_object_list',context,data_dict)

    model = context['model']
    session = context['session']

    only_current = data_dict.get('only_current',True)
    source_id = data_dict.get('source_id',False)

    query = session.query(HarvestObject)

    if source_id:
        query = query.filter(HarvestObject.source_id==source_id)

    if only_current:
        query = query.filter(HarvestObject.current==True)

    objects = query.all()

    return [getattr(obj,'id') for obj in objects]

@side_effect_free
def harvesters_info_show(context,data_dict):

    check_access('harvesters_info_show',context,data_dict)

    available_harvesters = []
    for harvester in PluginImplementations(IHarvester):
        info = harvester.info()
        if not info or 'name' not in info:
            log.error('Harvester %r does not provide the harvester name in the info response' % str(harvester))
            continue
        info['show_config'] = (info.get('form_config_interface','') == 'Text')
        available_harvesters.append(info)

    return available_harvesters

def _get_sources_for_user(context,data_dict):

    model = context['model']
    session = context['session']
    user = context.get('user','')

    only_active = data_dict.get('only_active',False)
    only_to_run = data_dict.get('only_to_run',False)

    query = session.query(HarvestSource) \
                .order_by(HarvestSource.created.desc())

    if only_active:
        query = query.filter(HarvestSource.active==True) \

    if only_to_run:
        query = query.filter(HarvestSource.frequency!='MANUAL')
        query = query.filter(or_(HarvestSource.next_run<=datetime.datetime.utcnow(),
                                 HarvestSource.next_run==None)
                            )

    user_obj = User.get(user)
    # Sysadmins will get all sources
    if user_obj and not user_obj.sysadmin:
        # This only applies to a non sysadmin user when using the
        # publisher auth profile. When using the default profile,
        # normal users will never arrive at this point, but even if they
        # do, they will get an empty list.

        publisher_filters = []
        publishers_for_the_user = user_obj.get_groups(u'publisher')
        for publisher_id in [g.id for g in publishers_for_the_user]:
            publisher_filters.append(HarvestSource.publisher_id==publisher_id)

        if len(publisher_filters):
            query = query.filter(or_(*publisher_filters))
        else:
            # This user does not belong to a publisher yet, no sources for him/her
            return []

        log.debug('User %s with publishers %r has Harvest Sources: %r',
                  user, publishers_for_the_user, [(hs.id, hs.url) for hs in query])

    sources = query.all()

    return sources

