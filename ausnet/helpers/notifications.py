import os
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import logging

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/mail.log'

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('connectionlogs')

def send_email_notification_of_failure(source_name, source_url, extraction_date, error_msg):
    sender_email = "virajthakrar997@gmail.com"
    receiver_email = ["virajthakrar97@gmail.com","iot@senstra.com.au"]
    password = "nxyhvdeovumptvfv"

    message = MIMEMultipart("alternative")
    message["Subject"] = "{0} : Extraction Failed".format(source_name)
    message["From"] = sender_email
    message["To"] = ', '.join(receiver_email)

    # Create the plain-text and HTML version of your message
    html = """\
    <html>
    <body>
        <p>Hi,<br>
        Find the failed extraction below and check logs on server for what's wrong:<br>
        <ul>
            <li>Source Name: {0}</li>
            <li>Source URL: {1}</li>
            <li>Extraction Date: {2}</li>
            <li>Error Message: {3}</li>
        </ul>
        </p>
    </body>
    </html>
    """.format(source_name, source_url, extraction_date, error_msg)

    # Turn these into plain/html MIMEText objects
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part2)

    # Create secure connection with server and send email
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(
                sender_email, receiver_email, message.as_string()
            )
        logging.info("Mail sent sucessfully")
    except Exception as error:
        logging.error("Unable to send email, please find the following warning message for extraction failure details: {0}".format(error))
        logging.warning("Since email delivery failed, the failure details are: source_name: {0}, source_url: {1}, extraction_date: {2}, error_message: {3}".format(source_name, source_url, extraction_date, error_msg))