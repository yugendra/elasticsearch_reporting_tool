from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import os.path
from prettytable import PrettyTable

from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import letter
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, \
    Table, TableStyle
from operator import itemgetter

class DataToPdf():
    """
    Export a list of dictionaries to a table in a PDF file.
    """

    def __init__(self, fields, data, sort_by=None, title=None):
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
        self.data = data
        self.title = title
        self.sort_by = sort_by

    def export(self, filename, data_align='LEFT', table_halign='LEFT'):
        """
        Export the data to a PDF file.

        Arguments:
            filename - The filename for the generated PDF file.
            data_align - The alignment of the data inside the table (eg.
                'LEFT', 'CENTER', 'RIGHT')
            table_halign - Horizontal alignment of the table on the page
                (eg. 'LEFT', 'CENTER', 'RIGHT')
        """
        doc = SimpleDocTemplate(filename, pagesize=letter)

        self.styles = getSampleStyleSheet()
        styleH = self.styles['Heading1']

        story = []

        if self.title:
            story.append(Paragraph(self.title, styleH))
            story.append(Spacer(1, 0.25 * inch))

        if self.sort_by:
            reverse_order = False
            if (str(self.sort_by[1]).upper() == 'DESC'):
                reverse_order = True

            self.data = sorted(self.data,
                               key=itemgetter(self.sort_by[0]),
                               reverse=reverse_order)

        #converted_data = self.__convert_data()
        #print converted_data
        #data1 = Paragraph('saasassaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', self.styles["BodyText"])
        #data2 = Paragraph('saasassaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', self.styles["BodyText"])
        #converted_data = [['ssd','sds'],[data1,data2]]
        
        converted_data = [['Sender', 'Recipient', 'Subject']]
        for d in self.data:
            sender = d['sender']
            try:
                recipient = d['recipient']
            except:
                pass
                
            try:
                subject = d['subject']
            except:
                pass
            data1 = [Paragraph(sender, self.styles['BodyText']), Paragraph(recipient, self.styles['BodyText']), Paragraph(subject, self.styles['BodyText'])]
            print data1
            converted_data.append(data1)
            
        
        table = Table(converted_data, hAlign=table_halign, colWidths=[2 * inch, 2 * inch, 3 * inch])
        table.setStyle(TableStyle([
            ('FONT', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('ALIGN',(0, 0),(0,-1), data_align),
            ('INNERGRID', (0, 0), (-1, -1), 0.50, colors.black),
            ('BOX', (0,0), (-1,-1), 0.25, colors.black),
        ]))

        story.append(table)
        doc.build(story)

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

        '''
        for d in self.data:
            try:
                for k in keys:
                    data = Paragraph(d[k], self.styles['Normal'])
                    print d[k]
                    new_data.append(data)
            except:
                pass
        print new_data
        '''
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

def fetch_data():
    '''
    Fetch the data from elasticsearch.
    Apply filter on data before fetching.
    Returns only specified fields
    Convert fetched objects into json format and write into the file.
    '''
    client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    s = Search(using=client, index='filebeat*')
    s = s.query('match', sender='subrat.ghosh@athagroup.in')
    s = s.source(['sender','recipient','subject'])
    
    #fh = open('data.json', 'w')
    pt = PrettyTable(["Sender", "Recipient", "Subject"])
    pt.align["Sender"] = "l"
    pt.align["Recipient"] = "l"
    pt.align["Subject"] = "l"
    pt.padding_width = 1
    
    data=[]
     
    for hit in s.scan():
        #fh.write(json.dumps(hit, default=lambda o: o.__dict__))
        #print(json.dumps(hit, default=lambda o: o.__dict__))
        data.append(hit.__dict__['_d_'])
    
    fields = (
        ('sender', 'Sender'),
        ('recipient', 'Recipient'),
        ('subject', 'Subject'),
    )
    
    doc = DataToPdf(fields, data, sort_by=('sender', 'ASC'),
                    title='Sender List')
    doc.export('SenderList.pdf')

    #fh.close()
        
if __name__ == "__main__":
    file_cleanup()
    fetch_data()