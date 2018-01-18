from report import CreateReport
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

def get_user_list(client):
    sender = Search(using=client, index='filebeat*')
    sender = sender.query('match', MTA='postfix-outgoing')
    sender = sender.query('regexp', SENDER='@athagroup.in')
    sender = sender.source(['SENDER'])
    data=[]
    for hit in sender.scan():
        data.append(hit.__dict__['_d_']['SENDER'])

    sender_list = sorted(set(data))
    
    recipient = Search(using=client, index='filebeat*')
    recipient = recipient.query('match', MTA='postfix-outgoing')
    recipient = recipient.query('regexp', RECIPIENT='@athagroup.in')
    recipient = recipient.source(['RECIPIENT'])
    data=[]
    for hit in recipient.scan():
        data.append(hit.__dict__['_d_']['RECIPIENT'])

    recipient_list = sorted(set(data))
    
    user_list = []
    user_list.extend(sender_list)
    user_list.extend(recipient_list)
    
    return sorted(set(user_list))
    
def split_email(email): 
    user = email.split('@')
    return user[0]
    
def get_sender_data(client, user):
    user_id = split_email(user)
    sender = Search(using=client, index='filebeat*')
    q = Q('bool', must=[Q('match', SENDER=user_id)]) & Q('bool', must=[Q('match', SENDER="athagroup.in")])
    sender = sender.query(q)
    sender = sender.source(['SENDER','RECIPIENT', 'SUBJECT', 'SIZE', 'ATTACHMENT', '@timestamp'])
    data=[]
    for hit in sender.scan():
        if 'bounce' not in hit.__dict__['_d_']['SENDER'] and '=athagroup.in' not in hit.__dict__['_d_']['SENDER']:
            record = []
            record.append(hit.__dict__['_d_']['@timestamp'])
            record.append(hit.__dict__['_d_']['RECIPIENT'])
            record.append(hit.__dict__['_d_']['SUBJECT'])
            get_size_in_mb(hit.__dict__['_d_']['SIZE'])
            record.append(hit.__dict__['_d_']['SIZE'])
            record.append(hit.__dict__['_d_']['ATTACHMENT'])
            data.append(record)
        
    return data
    
def get_recipient_data(client, user):
    user_id = split_email(user)
    recipient = Search(using=client, index='filebeat*')
    q = Q('bool', must=[Q('match', RECIPIENT=user_id)]) & Q('bool', must=[Q('match', RECIPIENT="athagroup.in")]) & Q('bool', must=[Q('match', MTA="QMAIL")])
    recipient = recipient.query(q)
    recipient = recipient.source(['SENDER','RECIPIENT', 'SUBJECT', 'SIZE', 'ATTACHMENT', '@timestamp'])
    data=[]
    for hit in recipient.scan():
        if 'bounce' not in hit.__dict__['_d_']['RECIPIENT'] and '=athagroup.in' not in hit.__dict__['_d_']['RECIPIENT']:
            record = []
            record.append(hit.__dict__['_d_']['@timestamp'])
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
        if not '=athagroup.in' in user:
            doc.add_user_header(user)
            doc.add_user_header("Mail Sent")
            doc.add_table_data(sender_fields, style='THeader')
            doc.add_table_data(get_sender_data(client, user), style='TData')
            doc.add_user_header("Mail Received")
            doc.add_table_data(recipient_fields, style='THeader')
            doc.add_table_data(get_recipient_data(client, user), style='TData')
            doc.add_user_header(" ")
    
    doc.create()
        
if __name__ == "__main__":
    file_cleanup()
    fetch_data()