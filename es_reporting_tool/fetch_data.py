from report import CreateReport

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

import os.path
from prettytable import PrettyTable

from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import landscape, A4
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, \
    Table, TableStyle
from operator import itemgetter

class DataToPdf():
    """
    Export a list of dictionaries to a table in a PDF file.
    """

    def __init__(self, fields, sort_by=None, title=None):
        """
        Arguments:
            fields - A tuple of tuples ((fieldname/key, display_name))
                specifying the fieldname/key and corresponding display
                name for the table header.
            data - The data to insert to the table formatted as a list of
                dictionaries.
            sort_by - A tuple (sort_key, sort_order) specifying which field
                to sort by and the sort order ('ASC', 'DESC').
            title - The title to display at the beginning of the document.
        """
        self.fields = fields
        self.title = title
        self.sort_by = sort_by
        self.doc = SimpleDocTemplate('SenderList.pdf', pagesize=A4)
        self.styles = getSampleStyleSheet()
        self.styleH = self.styles['Heading1']
        self.story = []

    def export(self, sender, data, data_align='LEFT', table_halign='LEFT'):
        """
        Export the data to a PDF file.

        Arguments:
            filename - The filename for the generated PDF file.
            data_align - The alignment of the data inside the table (eg.
                'LEFT', 'CENTER', 'RIGHT')
            table_halign - Horizontal alignment of the table on the page
                (eg. 'LEFT', 'CENTER', 'RIGHT')
        """

        
        
        self.story.append(Paragraph(sender, self.styleH))

        '''
        if self.title:
            story.append(Paragraph(self.title, self.styleH))
            story.append(Spacer(1, 0.25 * inch))
        

        if self.sort_by:
            reverse_order = False
            if (str(self.sort_by[1]).upper() == 'DESC'):
                reverse_order = True

            data = sorted(data,
                               key=itemgetter(self.sort_by[0]),
                               reverse=reverse_order)
        '''

        converted_data = [['Recipient', 'Subject', 'Size', 'Attachment']]
        for d in data:
            #print d
            try:
                recipient = d['RECIPIENT']
            except:
                recipient = ''
                
            try:
                subject = d['SUBJECT']
            except:
                subject = ''
                
            try:
                size = d['SIZE']
            except:
                size = ''

            try:
                attachment = d['ATTACHMENT']
            except:
                attachment = ''
                
            data1 = [Paragraph(recipient, self.styles['BodyText']),
                        Paragraph(subject, self.styles['BodyText']),
                        Paragraph(size, self.styles['BodyText']),
                        Paragraph(attachment, self.styles['BodyText'])]
            #print data1
            converted_data.append(data1)
            
        table = Table(converted_data, hAlign=table_halign, colWidths=[1.5 * inch, 3 * inch, 1 * inch, 1.5 * inch])
        table.setStyle(TableStyle([
            ('FONT', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('ALIGN',(0, 0),(0,-1), data_align),
            ('INNERGRID', (0, 0), (-1, -1), 0.50, colors.black),
            ('BOX', (0,0), (-1,-1), 0.25, colors.black),
        ]))

        self.story.append(table)
        
    def build_doc(self):    
        print "In build doc"
        self.doc.build(self.story)

    def __convert_data(self):
        """
        Convert the list of dictionaries to a list of list to create
        the PDF table.
        """
        # Create 2 separate lists in the same order: one for the
        # list of keys and the other for the names to display in the
        # table header.
        keys, names = zip(*[[k, n] for k, n in self.fields])
        new_data = [names]

       
        new_data.append(Paragraph('text1', self.styles['BodyText']))
        return new_data

def file_cleanup():
    '''
    Remove old files and
    '''
    try:
        os.remove('data.json')
    except OSError:
        pass
        
    try:
        os.remove('data.csv')
    except OSError:
        pass
        
    try:
        os.remove('SenderList.pdf')
    except OSError:
        pass
        
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
    '''
    Fetch the data from elasticsearch.
    Apply filter on data before fetching.
    Returns only specified fields
    Convert fetched objects into json format and write into the file.
    '''
    
    client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    user_list = get_user_list(client)
    sender_fields = [['Time', 'Recipient', 'Subject', 'Size', 'Attachment']]
    recipient_fields = [['Time', 'Sender', 'Subject', 'Size', 'Attachment']]
    
    doc = CreateReport(title='report.pdf')
    
    for user in user_list:
        if not '=athagroup.in' in user:
            #print user
            doc.add_text(user)
            doc.add_text("Mail Sent")
            doc.add_table_headings(sender_fields)
            doc.add_table_headings(get_sender_data(client, user))
            doc.add_text("Mail Recived")
            doc.add_table_headings(recipient_fields)
            doc.add_table_headings(get_recipient_data(client, user))
    
    doc.create()
        
if __name__ == "__main__":
    file_cleanup()
    fetch_data()