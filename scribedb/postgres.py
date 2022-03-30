import logging

import time

from psycopg2 import Error, InterfaceError, connect
from mo_sql_parsing import parse, format
from typing import List
from rich import print as rprint


ESTIMATE_LOOP = 2
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
$$;

CREATE or replace AGGREGATE md5_agg (text ORDER BY anyelement)
(
  STYPE = text,
  SFUNC = {PG_FNAME},
  INITCOND = ''
);
"""


def log_psycopg2_exception(err):
    st = str(err)
    LOGGER.error("\npsycopg2 ERROR: %s", st)
    st = str(err.pgcode)
    LOGGER.error("pgcode: %s", st)


class Postgres:
    """Postgres connection params."""

    LOGGER.setLevel(getattr(logging, "INFO"))

    def execquery(self, qry: str):
        start=time.time_ns()
        resultset = None
        try:
            cur = self.conn.cursor()
            cur.execute(qry)
            if cur.rowcount > 0:
                resultset = cur.fetchone()
            cur.close()
            self.exec_duration = (time.time_ns() - start) / 1000000
            return resultset
        except (Error, InterfaceError) as err:
            self.close()
            log_psycopg2_exception(err)
            raise

    def __init__(self, qry, view_name):
        self._parsed = parse(qry)
        self.view_name = view_name
        self.qry = qry
        self.bucket = 0

    def prepare(self, host, port, username, password, dbname, sslmode):
        try:
            self.conn = connect(dbname=dbname, user=username, password=password, host=host, port=port, sslmode=sslmode)
            self.conn.autocommit = True
        except Exception as err:
            log_psycopg2_exception(err)
            self.conn = None
            raise
        self.execquery(str(PG_MD5_FN))

        self.create_view()
        self.num_rows = self.rowcount()

    def hash(self, start=0, stop=0):
        self.create_view(start, stop)
        sql = f"""SELECT md5_agg() WITHIN GROUP (ORDER BY {self.view_name}) FROM {self.view_name}"""
        tmp = self.execquery(sql)
        self._computed_hash = tmp[0]

    def explain(self, qry):
        st = self.execquery(qry)
        return self.exec_duration
        # sql = f"""explain (analyze, format json) {qry}"""
        # st = self.execquery(sql)[0][0][0]
        # return st["Execution Time"]

    def estimate_execution(self, qry_exec_time):
        """time = a.rows + round_trip
        rows = (time - (round_trip))/a
        """

        round_trip = 0
        time = 0
        rows = 0
        for i in range(ESTIMATE_LOOP):
            rprint(f"Pg:Estimating round_trip")
            round_trip = round_trip + self.explain(PG_ROUNDTRIP)
        round_trip = round_trip / ESTIMATE_LOOP
        for j in range(ESTIMATE_LOOP):
            r = (j + 1) + j * 100 - 2 * j
            self.create_view(0, r)
            for i in range(ESTIMATE_LOOP):
                sql = f"""SELECT md5_agg() WITHIN GROUP (ORDER BY {self.view_name}) FROM {self.view_name}"""
                rprint(f"Pg:Estimating for {r} rows")
                time = time + self.explain(sql)
                rows = rows + r

        tx = time / rows
        bucket = round((qry_exec_time - round_trip) / tx)

       # LOGGER.info(f"""Estimation of [{bucket}] rows could be computed in [{qry_exec_time}ms]""")

        self.create_view(0, bucket)
        sql = f"""SELECT md5_agg() WITHIN GROUP (ORDER BY {self.view_name}) FROM {self.view_name}"""
        runtime = self.explain(sql)

        while runtime<(qry_exec_time-20*qry_exec_time/100):
            if bucket >= self.num_rows // 2:
                break
            bucket = round(bucket * 2)
            self.create_view(0, bucket)
            sql = f"""SELECT md5_agg() WITHIN GROUP (ORDER BY {self.view_name}) FROM {self.view_name}"""
            runtime = self.explain(sql)

        #LOGGER.info(f"""Effective [{bucket}] rows are computed in [{runtime}ms]""")

        self.bucket = bucket

    def computed_hash(self):
        return self._computed_hash

    def drop_md5_fn(self):
        self.execquery("fdrop function {FNAME} cascade")

    def create_view(self, start:int=0, stop:int=0):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """
        stmt = f"""create or replace view {self.view_name} as {self.qry}"""
        if  start !=0 or stop != 0:
            sql = stmt + f" limit {stop} offset {start}"
        else:
            sql = stmt
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

    def rowcount(self):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """

        sql = f"""select count(*) from {self.view_name}"""
        ret = self.execquery(sql)
        return ret[0]

    def close(self):
        self.drop_view()
        self.conn.close()
