import logging
import os
import time
from typing import Annotated, List, Literal, Optional, Union

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from pydantic import PrivateAttr
from rich import print as rprint

from .base import DBBase

PG_ROUNDTRIP = "select 1;"

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


class Postgres(DBBase):
    """Postgres connection params."""

    type: Literal["postgres"]
    dbname: str
    sslmode: Optional[str]

    _roundtrip: str = PrivateAttr(default=PG_ROUNDTRIP)

    def get_dataset(self):
        return self._d7

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._hash_qry = f"""SELECT md5_agg() WITHIN GROUP (ORDER BY {self._view_name}) FROM {self._view_name}"""
        sqlUrl = URL.create(
            drivername="postgresql+psycopg2",
            username=self.username,
            password=os.getenv(self.password),
            host=self.host,
            port=self.port,
            database=self.dbname,
            # sslmode=self.sslmode,
        )
        try:
            _engine = create_engine(sqlUrl)
        except Exception as err:
            self.log_exception(err)
            self._engine = None
            raise
        self._conn = _engine.connect()

    def prepare(self):
        self.execquery(str(PG_MD5_FN))
        self.execquery(str(PG_MD5_AGG))
        self.create_view()
        self._num_rows = self.rowcount()
        rprint(f"{self.type} Counting rows:{self._num_rows}")

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

    def create_test_table(self):
        self.execquery(CREATE_TEST)
