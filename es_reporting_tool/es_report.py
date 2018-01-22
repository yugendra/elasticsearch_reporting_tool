from generate_report import CreateReport
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from send_report import send_email_report
from helper import report_name, format_time

def get_user_list(client):
    sender = Search(using=client, index='filebeat*')
    sender = sender.query('match', SENDER='athagroup.in')
    sender = sender.filter('range', **{'@timestamp':{'gte': 'now-30d' , 'lt': 'now'}})
    sender = sender.source(['SENDER'])
    data=[]
    for hit in sender.scan():
        data.append(hit.__dict__['_d_']['SENDER'])

    sender_list = sorted(set(data))
    
    recipient = Search(using=client, index='filebeat*')
    recipient = recipient.query('match', RECIPIENT='athagroup.in')
    recipient = recipient.filter('range', **{'@timestamp':{'gte': 'now-30d' , 'lt': 'now'}})
    recipient = recipient.source(['RECIPIENT'])
    data=[]
    for hit in recipient.scan():
        data.append(hit.__dict__['_d_']['RECIPIENT'])

    recipient_list = sorted(set(data))
    
    user_list = []
    user_list.extend(sender_list)
    user_list.extend(recipient_list)
    
    sorted_user_list = sorted(set(user_list))
    
    filtered_user_list = []
    
    for user in sorted_user_list:
        if '@athagroup.in' in user:
            filtered_user_list.append(user)
            
    return filtered_user_list
    
def get_sender_data(client, user):
    sender = Search(using=client, index='filebeat*')
    sender = sender.query('match', SENDER=user)
    sender = sender.filter('range', **{'@timestamp':{'gte': 'now-30d' , 'lt': 'now'}})
    sender = sender.source(['SENDER','RECIPIENT', 'SUBJECT', 'SIZE', 'ATTACHMENT', '@timestamp', 'MTA'])
    data=[]
    for hit in sender.scan():
        sender_ph = hit.__dict__['_d_']['SENDER']
        recipient_ph = hit.__dict__['_d_']['RECIPIENT']
        mta_ph = hit.__dict__['_d_']['MTA']
        if sender_ph == user and mta_ph == 'postfix-outgoing':
            record = []
            time = format_time(hit.__dict__['_d_']['@timestamp'])
            record.append(time)
            record.append(hit.__dict__['_d_']['RECIPIENT'])
            record.append(hit.__dict__['_d_']['SUBJECT'])
            record.append(hit.__dict__['_d_']['SIZE'])
            record.append(hit.__dict__['_d_']['ATTACHMENT'])
            data.append(record)
        if sender_ph == user and mta_ph == 'QMAIL' and '@athagroup.in' in recipient_ph:
            record = []
            time = format_time(hit.__dict__['_d_']['@timestamp'])
            record.append(time)
            record.append(hit.__dict__['_d_']['RECIPIENT'])
            record.append(hit.__dict__['_d_']['SUBJECT'])
            record.append(hit.__dict__['_d_']['SIZE'])
            record.append(hit.__dict__['_d_']['ATTACHMENT'])
            data.append(record)
        
    return data
    
def get_recipient_data(client, user):
    recipient = Search(using=client, index='filebeat*')
    recipient = recipient.query('match', RECIPIENT=user)
    recipient = recipient.filter('range', **{'@timestamp':{'gte': 'now-30d' , 'lt': 'now'}})
    recipient = recipient.source(['SENDER','RECIPIENT', 'SUBJECT', 'SIZE', 'ATTACHMENT', '@timestamp'])
    data=[]
    for hit in recipient.scan():
        if hit.__dict__['_d_']['RECIPIENT'] == user:
            record = []
            time = format_time(hit.__dict__['_d_']['@timestamp'])
            record.append(time)
            record.append(hit.__dict__['_d_']['SENDER'])
            record.append(hit.__dict__['_d_']['SUBJECT'])
            record.append(hit.__dict__['_d_']['SIZE'])
            record.append(hit.__dict__['_d_']['ATTACHMENT'])
            data.append(record)
        
    return data
    
def fetch_data():
    client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    user_list = get_user_list(client)
    sender_fields = [['Time', 'Recipient', 'Subject', 'Size Bytes', 'Attachment']]
    recipient_fields = [['Time', 'Sender', 'Subject', 'Size Bytes', 'Attachment']]
    
    report_file_name = report_name()
    doc = CreateReport(title=report_file_name)
    doc.add_report_header('Athagroup.in')
    
    for user in user_list:
        doc.add_user_header(user)
        doc.add_user_header("Mail Sent")
        doc.add_table_data(sender_fields, style='THeader')
        sender_data = get_sender_data(client, user)
        if sender_data: doc.add_table_data(sender_data, style='TData')
        doc.add_user_header("Mail Received")
        doc.add_table_data(recipient_fields, style='THeader')
        recipient_data = get_recipient_data(client, user)
        if recipient_data: doc.add_table_data(recipient_data, style='TData')
        doc.add_user_header(" ")
        
    
    doc.create()
    send_email_report(report_file_name)
        
if __name__ == "__main__":
    fetch_data()