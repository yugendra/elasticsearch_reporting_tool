import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders
from config import email_config as cfg
 
def send_email_report(report_file):
    fromaddr = cfg['email_fromaddr']
    toaddr = cfg['email_toaddr']
    
    msg = MIMEMultipart()
    
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = cfg['email_subject']
    
    body = cfg['email_body']
    
    msg.attach(MIMEText(body, 'plain'))
    
    filename = report_file.split('/')[-1]
    attachment = open(report_file, "rb")
    
    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
    
    msg.attach(part)
    
    server = smtplib.SMTP(cfg['email_server'], cfg['email_port'])
    server.starttls()
    server.login(fromaddr, cfg['email_password'])
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()