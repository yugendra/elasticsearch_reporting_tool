import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders
 
def send_email_report(report_file):
    fromaddr = "ykhonde@gmail.com"
    toaddr = "yugendra.mail@gmail.com"
    
    msg = MIMEMultipart()
    
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "athagroup.in email report."
    
    body = "Please find report in attachments."
    
    msg.attach(MIMEText(body, 'plain'))
    
    filename = report_file.split('/')[-1]
    attachment = open(report_file, "rb")
    
    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
    
    msg.attach(part)
    
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, "w205sony")
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()