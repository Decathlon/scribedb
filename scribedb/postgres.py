import logging
import os
import time
from typing import Annotated, List, Literal, Optional, Union

from mo_sql_parsing import format, parse
from psycopg2 import Error, InterfaceError, connect
from psycopg2.extensions import connection
from pydantic import PrivateAttr
from rich import print as rprint

from .base import DBBase


LOGGER = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)

PG_ROUNDTRIP = "select 1"

PG_FNAME = "md5_agg_sfunc"
PG_MD5_FN = f"""CREATE or replace FUNCTION {PG_FNAME}(text, anyelement)
RETURNS text
LANGUAGE sql
AS
$$
  SELECT upper(md5($1 || $2::text))
$$"""

PG_MD5_AGG = f"""CREATE or replace AGGREGATE md5_agg (ORDER BY anyelement)
(
  STYPE = text,
  SFUNC = {PG_FNAME},
  INITCOND = ''
)"""


def log_psycopg2_exception(err):
    st = str(err)
    LOGGER.error("\npsycopg2 ERROR: %s", st)
    st = str(err.pgcode)
    LOGGER.error("pgcode: %s", st)


class Postgres(DBBase):
    """Postgres connection params."""

    type: Literal["postgres"]
    dbname: str
    sslmode: Optional[str]

    _conn: connection = PrivateAttr()
    _roundtrip: str = PrivateAttr(default=PG_ROUNDTRIP)

    def execquery(self, qry: str):
        start = time.time_ns()
        resultset = None
        try:
            cur = self._conn.cursor()
            cur.execute(qry)
            if cur.rowcount > 0:
                resultset = cur.fetchone()
            cur.close()
            self._exec_duration = (time.time_ns() - start) / 1000000
            return resultset
        except (Error, InterfaceError) as err:
            self.close()
            log_psycopg2_exception(err)
            raise

    def execquery_all(self, qry: str):
        start = time.time_ns()
        resultset = None
        try:
            cur = self._conn.cursor()
            cur.execute(qry)
            if cur.rowcount > 0:
                resultset = cur.fetchall()
            cur.close()
            self._exec_duration = (time.time_ns() - start) / 1000000
        except (Error, InterfaceError) as err:
            self.close()
            log_psycopg2_exception(err)
            raise
        self._d7 = resultset

    def get_dataset(self):
        return self._d7

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._hash_qry = f"""SELECT md5_agg() WITHIN GROUP (ORDER BY {self._view_name}) FROM {self._view_name}"""

    def prepare(self):
        try:
            self._conn = connect(
                dbname=self.dbname,
                user=self.username,
                password=os.getenv(self.password),
                host=self.host,
                port=self.port,
                sslmode=self.sslmode,
            )
            self._conn.autocommit = True
        except Exception as err:
            log_psycopg2_exception(err)
            self._conn = None
            raise
        self.execquery(str(PG_MD5_FN))
        self.execquery(str(PG_MD5_AGG))
        self._conn.commit()
        self.create_view()
        rprint(f"{self.type} Counting rows")
        self._num_rows = self.rowcount()

    def drop_md5_fn(self):
        self.execquery(f"drop function {PG_FNAME} cascade")

    def create_view(self, start: int = 0, stop: int = 0):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """
        stmt = f"""create or replace view {self._view_name} as {self.qry}"""
        if start != 0 or stop != 0:
            sql = stmt + f" limit {stop} offset {start}"
        else:
            sql = stmt
        self.execquery(sql)
        self._conn.commit()
