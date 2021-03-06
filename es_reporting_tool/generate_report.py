from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import A3
from reportlab.platypus import Paragraph, SimpleDocTemplate, Table, TableStyle
from reportlab.lib.enums import TA_CENTER

import datetime

class CreateReport():
    def __init__(self, title='SampleReport.pdf'):
        self.title = title
        self.doc = SimpleDocTemplate(self.title, pagesize=A3)
        self.styles = getSampleStyleSheet()
        self.reportHeaderStyle = self.styles['Heading1']
        self.reportHeaderStyle.alignment  = TA_CENTER
        self.reportHeaerStyle = self.styles['Heading1']
        self.userHeaderStyle = self.styles['Heading2']
        self.TableHeaderStyle = self.styles['Heading3']
        self.TableHeaderStyle.alignment  = TA_CENTER
        self.normalStyle = self.styles['Normal']
        self.normalStyle.wordWrap = 'CJK'
        self.story = []
        
    def wrap_text(self, data, style):
        row = []
        for filed in data:
            row.append(Paragraph(filed, style))
        return row
    
    def add_report_header(self, data):
        self.story.append(Paragraph(data, self.reportHeaderStyle))
    
    def add_user_header(self, data):
        self.story.append(Paragraph(data, self.userHeaderStyle))
    
    def add_table_data(self, data, style='TData'):
        if style == 'THeader':
            style = self.TableHeaderStyle
        else:
            style = self.normalStyle

        for i in range(len(data)):
            if data[i][0] == "Time":
                continue
            for j in range(i+1, len(data)):
                iDate = datetime.datetime.strptime(data[i][0], "%Y-%m-%d %H:%M:%S")
                jDate = datetime.datetime.strptime(data[j][0], "%Y-%m-%d %H:%M:%S")
                if iDate > jDate:
                    tmp = data[i]
                    data[i] = data[j]
                    data[j] = tmp
            

        table_halign='LEFT'
        data_align='LEFT'
        data1 = []
        for row in data:
            data1.append(self.wrap_text(row, style))
        
        table = Table(data1, hAlign=table_halign, colWidths=[1 * inch, 1.5 * inch, 3 * inch, 0.7 * inch, 3.5 * inch])
        
        table.setStyle(TableStyle([
            ('FONT', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('ALIGN',(0, 0),(0,-1), data_align),
            ('INNERGRID', (0, 0), (-1, -1), 0.50, colors.black),
            ('BOX', (0,0), (-1,-1), 0.25, colors.black),
        ]))
        
        self.story.append(table)
        
    def create(self):
        self.doc.build(self.story)
