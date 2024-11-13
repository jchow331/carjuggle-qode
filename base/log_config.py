from logging.config import dictConfig

dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {
            'format': '[%(name)s:%(lineno)s] %(levelname)s: %(message)s'
        },
        'file': {
            'format': '[%(levelname)s] %(asctime)s [%(name)s:%(lineno)s]: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'console'
        },
        'file': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'file',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 10,
            'filename': './logs/car_juggle_scrapers.log'
        },
    },
    'loggers': {
        '': {
            'level': 'INFO',
            'handlers': ['console', 'file']
        }
    }
})
