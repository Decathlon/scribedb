import psycopg2
import logging

LOGGER = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


class Postgres:
    def __init__(self, logLevel, cxString):
        LOGGER.setLevel(getattr(logging, logLevel))
        try:
            self.conn = psycopg2.connect(cxString)
            self.conn.autocommit = True
        except Exception as err:
            LOGGER.error("\npsycopg2 ERROR:[%s] CODE:[%s]", err, err.pgcode)
            self.conn = None
            raise
