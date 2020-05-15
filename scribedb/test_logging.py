import logging
import logstash
from logstash_formatter import LogstashFormatter


host = 'localhost'
port = 5044


formatter = LogstashFormatter()

logger = logging.getLogger('simple_example')
logger.setLevel(logging.INFO)

dh = logstash.TCPLogstashHandler(host, port, version=1)
dh.setLevel(logging.INFO)
dh.setFormatter(formatter)
logger.addHandler(dh)


ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

# 'application' code
logger.debug('debug message')
logger.info('info message')
logger.warning('warn message')
logger.error('error message')
logger.critical('critical message')
