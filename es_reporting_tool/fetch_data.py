from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import os.path
from prettytable import PrettyTable

#from reportlab.pdfgen import canvas
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
        
def get_sender_list(client):
    s = Search(using=client, index='filebeat*')
    s = s.query('match', MTA='postfix-outgoing')
    s = s.query('regexp', SENDER='@athagroup.in')
    s = s.source(['SENDER'])
    data=[]
    for hit in s.scan():
        data.append(hit.__dict__['_d_']['SENDER'])

    return sorted(set(data))
    
def fetch_data():
    '''
    Fetch the data from elasticsearch.
    Apply filter on data before fetching.
    Returns only specified fields
    Convert fetched objects into json format and write into the file.
    '''
    client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    sender_list = get_sender_list(client)
    
    fields = (
        ('RECIPIENT', 'Recipient'),
        ('SUBJECT', 'Subject'),
        ('SIZE', 'Size'),
        ('ATTACHMENT', 'Attachment'),
    )
    
    doc = DataToPdf(fields, sort_by=('SENDER', 'ASC'),
                    title='Sender List')
    
    for sender in sender_list:
        s = Search(using=client, index='filebeat*')
        s = s.query('match', MTA='postfix-outgoing')
        s = s.query('match', SENDER=sender)
        s = s.source(['RECIPIENT','SUBJECT', 'SIZE', 'ATTACHMENT'])
        
        data=[]
     
        for hit in s.scan():
            data.append(hit.__dict__['_d_'])
            
        doc.export(sender, data)
    
    doc.build_doc()
    
    #fh = open('data.json', 'w')
    '''
    pt = PrettyTable(["Sender", "Recipient", "Subject", "Size", "Attachment"])
    pt.align["Sender"] = "l"
    pt.align["Recipient"] = "l"
    pt.align["Subject"] = "l"
    pt.align["Size"] = "l"
    pt.align["Attachment"] = "l"
    pt.padding_width = 1
    '''
    
    

    #fh.close()
        
if __name__ == "__main__":
    file_cleanup()
    fetch_data()