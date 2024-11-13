import email
import smtplib
import logging, logging.handlers 
from functools import wraps
from .custom_handlers import TLSSMTPHandler
from decouple import config

EMAIL_HOST = config('EMAIL_HOST')
EMAIL_HOST_USER = config('EMAIL_HOST_USER')
EMAIL_HOST_PASSWORD = config('EMAIL_HOST_PASSWORD')
EMAIL_PORT = config('EMAIL_PORT')
FROM_EMAIL = config('FROM_EMAIL')


def create_logger(logger_name, email_subject, email_toaddrs):

    # create a logger object
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.ERROR)

    # create an email handler object
    smtp_handler = logging.handlers.SMTPHandler(mailhost=(EMAIL_HOST, EMAIL_PORT),
                                                fromaddr=FROM_EMAIL,
                                                toaddrs=email_toaddrs,
                                                subject=email_subject,
                                                credentials=(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD),
                                                secure=None)

    tls_smtp_handler = TLSSMTPHandler(mailhost=(EMAIL_HOST, EMAIL_PORT),
                                      fromaddr=FROM_EMAIL,
                                      toaddrs=email_toaddrs,
                                      subject=email_subject,
                                      credentials=(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD))

    # create a formatter object
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)

    # set formatter and handlers
    smtp_handler.setFormatter(formatter)
    logger.addHandler(tls_smtp_handler)

    return logger


def handle_exceptions(logger_name, email_subject, email_toaddrs):
    logger = create_logger(logger_name, email_subject, email_toaddrs)
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                issue = f'Exception in {func.__name__} \n'
                issue = f'{issue} {"="*100} \n'
                logger.exception(issue)
                raise
        return wrapper
    return decorator

def scrapper_error_notification(email_subject, email_toaddrs, message):
    smtp = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
    smtp.ehlo()  # for tls add this line
    smtp.starttls()  # for tls add this line
    smtp.ehlo()  # for tls add this line
    smtp.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    msg = "Subject: %s\n%s" % (email_subject, message)
    smtp.sendmail(FROM_EMAIL, email_toaddrs, msg)
    smtp.quit()

