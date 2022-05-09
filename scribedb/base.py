from ast import Str
import logging
import random
from typing import Annotated, Union
import time
from rich import print as rprint

from mo_sql_parsing import parse
from pydantic import BaseModel, Field, PrivateAttr
from sqlalchemy.engine.base import Connection


PREFIX = "scdb_"
ASCII_LETTER = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
ROW_LIMIT = 50
# QRY_EXECUTION_TIME = 5000
ESTIMATE_LOOP = 3


def random_char(y):
    tmp = "".join(random.choice(ASCII_LETTER) for x in range(y))
    return PREFIX + tmp


LOGGER = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


class DBBase(BaseModel):
    """DBBase with common attributes."""

    host: str
    port: int
    username: str
    password: str
    qry: str

    _view_name: str = PrivateAttr(default=random_char(20))  # "scdb_test"
    #    _qry_exec_time: int = PrivateAttr(default=QRY_EXECUTION_TIME)
    _bucket: int = PrivateAttr(default=0)
    _num_rows: int = PrivateAttr(default=0)
    _exec_duration: int = PrivateAttr(default=0)
    _computed_hash: str = PrivateAttr(default="")
    _hash_qry: str = PrivateAttr()

    _conn: Connection = PrivateAttr()

    _d7: list = PrivateAttr(default=[])

    def log_exception(self, err):
        LOGGER.error("\ora ERROR: %s", err)
        st = err.code
        LOGGER.error("code: %s", st)

    def execquery(self, qry: str):
        start = time.time_ns()
        resultset = None
        try:
            result = self._conn.execute(qry)
            if result.returns_rows:
                resultset = result.fetchall()
            self._exec_duration = (time.time_ns() - start) / 1000000
            return resultset
        except Exception as err:
            self.log_exception(err)
            raise

    def colcount(self):
        return len(parse(self.qry)["select"])

    def get_exec_duration(self):
        return self._exec_duration

    def get_bucket(self):
        return self._bucket

    def get_d7_num_rows(self):
        return self._num_rows

    def computed_hash(self):
        return self._computed_hash

    def rowcount(self):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """

        sql = f"""select count(*) from {self._view_name}"""
        ret = self.execquery(sql)
        return ret[0][0]

    def drop_view(self):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """

        sql = f"""drop view {self._view_name}"""
        self.execquery(sql)

    def drop_objects(self):
        pass

    def estimate_bucket_size(self, qry_exec_time):
        """time = a.rows + round_trip
        rows = (time - (round_trip))/a
        """

        def explain(qry):
            ret = 0
            st = self.execquery(qry)
            if st[0] != None:
                ret = self._exec_duration
            else:
                raise ValueError(f"""Returned value is null.""") from Exception
            return ret

        round_trip = 0
        time = 0
        rows = 0
        for i in range(ESTIMATE_LOOP):
            rprint(f"{self.type} Estimating round_trip, N° {i+1}")
            round_trip = round_trip + explain(self._roundtrip)
        round_trip = round_trip / ESTIMATE_LOOP
        for j in range(ESTIMATE_LOOP):
            r = (j + 1) + j * 100 - 2 * j
            self.create_view(0, r)
            for i in range(ESTIMATE_LOOP):
                rprint(f"{self.type} Estimating for {r} rows, N° {i+1}")
                time = time + explain(self._hash_qry)
                rows = rows + r

        tx = time / rows
        bucket = round((qry_exec_time - round_trip) / tx)

        # LOGGER.info(f"""Estimation of [{bucket}] rows could be computed in [{qry_exec_time}ms]""")

        self.create_view(0, bucket)
        runtime = explain(self._hash_qry)

        while runtime < (qry_exec_time - 20 * qry_exec_time / 100):
            if bucket >= self._num_rows // 2:
                break
            bucket = round(bucket * 2)
            self.create_view(0, bucket)
            runtime = explain(self._hash_qry)

        # LOGGER.info(f"""Effective [{bucket}] rows are computed in [{runtime}ms]""")

        self._bucket = bucket

    def hash(self, start=0, stop=0):
        self.create_view(start, stop)
        tmp = self.execquery(self._hash_qry)
        self._computed_hash = tmp[0][0]

    def retreive_dataset(self):
        sql = f"""select * from {self._view_name}"""
        self._d7 = self.execquery(sql)

    def get_dataset(self):
        return self._d7

    def close(self):
        self.drop_view()
        self.drop_objects()
        self._conn.close()
