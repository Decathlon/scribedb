import logging
import random

from pydantic import PrivateAttr
from psycopg2 import Error, InterfaceError, connect
from mo_sql_parsing import parse
from typing import Literal, Optional


LOGGER = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


PG_FNAME = "md5_agg_sfunc"
PG_MD5_FN = f"""CREATE or replace FUNCTION {PG_FNAME}(text, anyelement)
RETURNS text
LANGUAGE sql
AS
$$
  SELECT upper(md5($1 || $2::text))
$$;

CREATE or replace AGGREGATE md5_agg (text ORDER BY anyelement)
(
  STYPE = text,
  SFUNC = {PG_FNAME},
  INITCOND = ''
);
"""

PREFIX = "scdb_"
ASCII_LETTER = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def random_char(y):
    tmp = "".join(random.choice(ASCII_LETTER) for x in range(y))
    return tmp


def log_psycopg2_exception(err):
    st = str(err)
    LOGGER.error("\npsycopg2 ERROR: %s", st)
    st = str(err.pgcode)
    LOGGER.error("pgcode: %s", st)


class Postgres:
    """Postgres connection params."""

    LOGGER.setLevel(getattr(logging, "INFO"))

    view_name = random_char(20)
    computed_hash: str

    def execquery(self, qry: str):
        resultset = None
        try:
            cur = self.conn.cursor()
            cur.execute(qry)
            if cur.rowcount > 0:
                resultset = cur.fetchall()
            cur.close()
            return resultset
        except (Error, InterfaceError) as err:
            self.close()
            log_psycopg2_exception(err)
            raise

    def __init__(self, qry):
        self._parsed = parse(qry)
        self.qry = qry

    def prepare(self, host, port, username, password, dbname, sslmode):
        try:
            self.conn = connect(dbname=dbname, user=username, password=password, host=host, port=port, sslmode=sslmode)
            self.conn.autocommit = True
        except Exception as err:
            log_psycopg2_exception(err)
            self.conn = None
            raise
        self.execquery(str(PG_MD5_FN))
        self.create_view(self.qry)
        self.num_rows = self.rowcount()

    def hash(self):
        sql = f"""SELECT md5_agg() WITHIN GROUP (ORDER BY {self.view_name}) FROM {self.view_name}"""
        tmp = self.execquery(sql)
        self.computed_hash = tmp[0][0]
        return self.computed_hash

    def drop_md5_fn(self):
        self.execquery("fdrop function {FNAME} cascade")

    def create_view(self, qry):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """

        sql = f"""create or replace view {self.view_name} as {qry}"""
        self.execquery(sql)

    def drop_view(self):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """

        sql = f"""drop view {self.view_name}"""
        self.execquery(sql)

    def check_if_fn_exists(self, name: str):
        qry = f"select '{name}'::regproc"
        return self.execquery(qry)

    def colcount(self):
        return len(self._parsed["select"])

    def rowcount(self):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """

        sql = f"""select count(*) from {self.view_name}"""
        ret = self.execquery(sql)
        return ret[0][0]

    def close(self):
        self.drop_view()
        self.conn.close()
