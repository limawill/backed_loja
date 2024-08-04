import smtplib
from config import settings, logger
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart



class Mailhog:
    def __init__(self, smtp_host=settings.mailhog.smtp_host, smtp_port=settings.mailhog.smtp_port):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port

    def send_email(self, nome, email, titulo, corpo) -> bool:

        msg = MIMEMultipart()
        msg['From'] = f'{nome} <no-reply@example.com>'
        msg['To'] = email
        msg['Subject'] = titulo
        msg.attach(MIMEText(corpo, 'plain'))

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.sendmail(msg['From'], msg['To'], msg.as_string())
                return True
        except Exception as e:
            logger.error(f"Erro ao enviar email: {e}")
            return False
