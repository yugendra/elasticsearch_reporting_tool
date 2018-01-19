from report import CreateReport
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

def get_user_list(client):
    sender = Search(using=client, index='filebeat*')
    sender = sender.query('match', SENDER='athagroup.in')
    sender = sender.source(['SENDER'])
    data=[]
    for hit in sender.scan():
        data.append(hit.__dict__['_d_']['SENDER'])

    sender_list = sorted(set(data))
    
    recipient = Search(using=client, index='filebeat*')
    recipient = recipient.query('match', RECIPIENT='athagroup.in')
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
    
def split_email(email): 
    user = email.split('@')
    return user[0]
    
def format_time(time):
    time = time.rstrip('Z')
    split_time = time.split('T')
    new_time = split_time[0] + " " + split_time[1]
    return new_time
    
def get_sender_data(client, user):
    #user_id = split_email(user)
    sender = Search(using=client, index='filebeat*')
    #q = Q('bool', must=[Q('match', SENDER=user)]) #& Q('bool', must=[Q('match', SENDER="athagroup.in")])
    sender = sender.query('match', SENDER=user)
    sender = sender.source(['SENDER','RECIPIENT', 'SUBJECT', 'SIZE', 'ATTACHMENT', '@timestamp'])
    data=[]
    for hit in sender.scan():
        if hit.__dict__['_d_']['SENDER'] == user:
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
    #user_id = split_email(user)
    recipient = Search(using=client, index='filebeat*')
    #q = Q('bool', must=[Q('match', RECIPIENT=user_id)]) & Q('bool', must=[Q('match', RECIPIENT="athagroup.in")]) & Q('bool', must=[Q('match', MTA="QMAIL")])
    recipient = recipient.query('match', RECIPIENT=user)
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
    sender_fields = [['Time', 'Recipient', 'Subject', 'Size', 'Attachment']]
    recipient_fields = [['Time', 'Sender', 'Subject', 'Size', 'Attachment']]
    
    doc = CreateReport(title='report.pdf')
    doc.add_report_header('Athagroup.in')
    
    for user in user_list:
        print user
        
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
        
if __name__ == "__main__":
    fetch_data()