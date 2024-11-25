import smtplib
import configparser
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def read_config():
    config = configparser.ConfigParser()
    config.read('/opt/airflow/dags/email.config')
    return config['smtp']

def send_email_custom(to_email, subject, body):
    smtp_config = read_config()
    smtp_host = smtp_config['smtp_host']
    smtp_port = smtp_config['smtp_port']
    smtp_user = smtp_config['smtp_user']
    smtp_password = smtp_config['smtp_password']

    # Tạo nội dung email
    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Gửi email
    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

def notify_email(**kwargs):
    # Lấy thông tin task context
    task_instance = kwargs['ti']
    task_id = task_instance.task_id  # ID của task bị lỗi
    dag_id = task_instance.dag_id  # ID của DAG

    # Tùy chỉnh subject và body
    subject = f"Task {task_id} in DAG {dag_id} failed"
    body = f"""
        The task with ID '{task_id}' in DAG '{dag_id}' has failed.
        Please check the logs for more details.
    """

    # Gửi email thông báo
    send_email_custom("tranquanganh11022004@gmail.com", subject, body)
