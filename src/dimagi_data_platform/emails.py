import smtplib
import datetime
import logging

from dimagi_data_platform import incoming_data_tables, data_warehouse_db, conf

logger = logging.getLogger('dimagi_data_platform')
db = conf.PEEWEE_DB_CON

def send_email(gmail_pwd, subj, txt):
    #http://stackoverflow.com/questions/10147455/trying-to-send-email-gmail-as-mail-provider-using-python

    gmail_user = conf.EMAIL_FROM_USER
    FROM = conf.EMAIL_FROM_USER
    TO = conf.EMAILS_TO
    SUBJECT = subj
    TEXT = txt

    # Prepare actual message
    message = """\From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, ", ".join(TO), SUBJECT, TEXT)

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587) #or port 465 doesn't seem to work!
        server.ehlo()
        server.starttls()
        server.login(gmail_user, gmail_pwd)
        server.sendmail(FROM, TO, message)
        server.close()
        print 'successfully sent the email'
    except:
        print "failed to send email"

def send_initial_email(gmail_pwd, start_time, incremental):
    if incremental:
        incremental_msg = "All data will be pulled incrementally"
    else:
        incremental_msg = "Data will not be pulled incrementally and will start pulling from the beginning of time."
    msg = """A data pull was started {start_time} by {cc_user}.

{incremental_msg}
    """.format(start_time=start_time, cc_user=conf.CC_USER, incremental_msg=incremental_msg)
    send_email(gmail_pwd, "(Data Platform) A new data pull has begun", msg)


def send_intermediary_email(gmail_pwd, domain_list):
    num_domains = len(domain_list)
    msg = """The admin data pull has been completed. 

We will now begin pulling individual domain data for the following {num_domains} domains:
    {domain_list}
    """.format(num_domains=num_domains, domain_list=", ".join(domain_list))

    send_email(gmail_pwd, "(Data Platform) Pulling Domain Data", msg)

def domains_not_extracted(dt):
    query_txt = ("select name from domain where id not in"
                 " (select domain_id from hq_extract_log where extract_end > '{dt}'::timestamp);".format(dt=dt))
    cursor = db.execute_sql(query_txt)
    return [row[0] for row in cursor.fetchall()]

def domains_with_unimported_forms():
    query_txt = ("SELECT COUNT(*), incoming_form.domain"
                 " FROM public.incoming_form"
                 " WHERE incoming_form.imported != True AND incoming_form.received_on > '2014-11-01'"
                 " GROUP BY incoming_form.domain LIMIT 1000;")
    cursor = db.execute_sql(query_txt)
    return dict((row[1], int(row[0])) for row in cursor.fetchall())


def send_finish_email(gmail_pwd, domain_list, timing_dict, missed_extractions):
    timing_string = '\n'.join("%s: %r seconds" % (d, round(t, 2)) for (d, t) in timing_dict.iteritems())
    missed_extractions_string = '\n'.join("%s: %r" % (d, ", ".join(es)) for (d, es) in missed_extractions.iteritems())
    dt = datetime.datetime.now()
    dt = datetime.date(dt.year, dt.month, 1)
    nonextracted_domains = [d for d in domains_not_extracted(dt) if d in domain_list]
    nonloaded_mapping = domains_with_unimported_forms()
    nonloaded_mapping = ', '.join("%s: %r" % (d, f) for (d, f) in nonloaded_mapping.iteritems())
    msg = """
The data pull for domains has completed. Salesforce data will now be pulled. The build may start now.

_____________________________________________________________________________________________________________________________
(STEP 2 FAILURES) Here is a list of domains with extracted forms that have not yet been loaded into 
the normalized databases. Mapping from name of domain to number of unloaded forms:

{nonloaded_mapping} 

_____________________________________________________________________________________________________________________________
(STEP 1 FAILURES) Here is a list of domains with *form* information not extracted prior to {dt}:

{nonextracted_domains}

_____________________________________________________________________________________________________________________________
(STEP 1 FAILURES) Here is a list of missed extractions, aka which part of a domains extraction did not go through.

{missed_extractions}

_____________________________________________________________________________________________________________________________
(TIMING INFO) Here's a high level log of timing information. Timing for individual extractors and loaders are saved on disk.

{timing}
 
    """.format(dt=dt, nonextracted_domains=nonextracted_domains, nonloaded_mapping=nonloaded_mapping, 
               missed_extractions=missed_extractions_string, timing=timing_string)
    send_email(gmail_pwd, "(Data Platform) Domain Data Pull Completed", msg)
