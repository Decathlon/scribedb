import sys
import logging
import os
import random
import math

from abc import ABC, abstractmethod
from scribedb.command_line import parse_command_line
from scribedb.configuration import Configuration
from scribedb.oracle import Oracle
from scribedb.postgres import Postgres
from typing import Literal, Annotated, Union, List
from typing_extensions import Annotated
from threading import Thread
from pydantic import BaseModel, Field, PrivateAttr
from mo_sql_parsing import parse
from rich import print as rprint

from typing import Optional


LOGGER = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


PREFIX = "scdb_"
ASCII_LETTER = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
ROW_LIMIT = 50
QRY_EXECUTION_TIME = 1000


def random_char(y):
    tmp = "".join(random.choice(ASCII_LETTER) for x in range(y))
    return PREFIX + tmp


class DBBase(BaseModel):
    """DBBase with common attributes."""

    host: str
    port: int
    username: str
    password: str
    qry: str

    _view_name: str = PrivateAttr(default="scdb_test")  # random_char(20))
    _qry_exec_time: int = PrivateAttr(default=QRY_EXECUTION_TIME)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def colcount(self):
        return len(parse(self.qry)["select"])


class DBPostgres(DBBase):

    type: Literal["postgres"]
    dbname: str
    sslmode: Optional[str]

    _pg: Postgres = PrivateAttr()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._pg = Postgres(self.qry, self._view_name)

    def prepare(self):
        self._pg.prepare(self.host, self.port, self.username, os.getenv(self.password), self.dbname, self.sslmode)

    def rowcount(self):
        return self._pg.num_rows

    def hash(self, start:int=0, stop:int=0):
        return self._pg.hash(start, stop)

    def computed_hash(self):
        return self._pg.computed_hash()

    def estimate(self):
        self._pg.estimate_execution(self._qry_exec_time)

    def exec_duration(self):
        return round(self._pg.exec_duration,1)

    def get_bucket(self):
        return self._pg.bucket

    def close(self):
        return self._pg.close()


class DBOracle(DBBase):

    init_oracle_client: str
    type: Literal["oracle"]
    instance: Optional[str]
    service_name: Optional[str]

    _ora: Oracle = PrivateAttr()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ora = Oracle(self.qry, self._view_name)

    def prepare(self):
        self._ora.prepare(
            self.host, self.port, self.username, os.getenv(self.password), self.instance, self.service_name
        )

    def rowcount(self):
        return self._ora.num_rows

    def hash(self, start:int=0, stop:int=0):
        return self._ora.hash(start, stop)

    def computed_hash(self):
        return self._ora.computed_hash()

    def estimate(self):
        self._ora.estimate_execution(self._qry_exec_time)

    def exec_duration(self):
        return round(self._ora.exec_duration,1)

    def get_bucket(self):
        return self._ora.bucket

    def close(self):
        return self._ora.close()


Db = Annotated[Union[DBPostgres, DBOracle], Field(discriminator="type")]


class Rdbms(BaseModel):
    """Rdbms dataset."""

    db: Db


class Compare(BaseModel):
    """Compare dataset."""

    source: Rdbms
    target: Rdbms
    loglevel: str
    max_delta: Optional[int]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        source_colcount = self.source.db.colcount()
        target_colcount = self.target.db.colcount()

        if (source_colcount != target_colcount) and target_colcount > 0:
            rprint(
                f"[bold red]Nb Column are not identical on source ({source_colcount}) and target ({target_colcount}), see qry in config file "
            )
            raise ValueError(
                f"""Number of column in target ({target_colcount}) is <> from source ({source_colcount}) dataset are different."""
            ) from Exception
        rprint(f"[bold green]Nb Column are identical ({source_colcount}) on source and target")

        source_prepare_thread = Thread(target=self.source.db.prepare())
        target_prepare_thread = Thread(target=self.target.db.prepare())
        source_prepare_thread.start()
        target_prepare_thread.start()
        source_prepare_thread.join()
        target_prepare_thread.join()

        # self.source.db.prepare()
        # self.target.db.prepare()

        source_prepare_thread = Thread(target=self.source.db.estimate())
        target_prepare_thread = Thread(target=self.target.db.estimate())
        source_prepare_thread.start()
        target_prepare_thread.start()
        source_prepare_thread.join()
        target_prepare_thread.join()

        # self.source.db.estimate()
        # self.target.db.estimate()

        # LOGGER.info("source hashing: %s rows", self.source.db.rowcount())
        # LOGGER.info("target hashing: %s rows", self.target.db.rowcount())
        rprint(f"[green]Source[/green] can hash ({self.source.db.get_bucket()}) rows in {QRY_EXECUTION_TIME}ms num_rows:{self.source.db.rowcount()}")
        rprint(f"[green]Target[/green] can hash ({self.target.db.get_bucket()}) rows in {QRY_EXECUTION_TIME}ms num_rows:{self.target.db.rowcount()}")

        # source_hash_thread = Thread(target=self.source.db.hash())
        # target_hash_thread = Thread(target=self.target.db.hash())
        # source_hash_thread.start()
        # target_hash_thread.start()
        # source_hash_thread.join()
        # target_hash_thread.join()

        bucket = min(self.source.db.get_bucket(), self.target.db.get_bucket())
        rows = max(self.source.db.rowcount(), self.target.db.rowcount())
        loops = math.ceil(rows / bucket)
        est_time = loops * QRY_EXECUTION_TIME // 1000
        rprint(f"[green]Total estimated time: [{est_time}]s")
        total_duration = 0
        for i in range(loops):
            source_prepare_thread = Thread(target=self.source.db.hash(i * bucket, (i + 1) * bucket))
            target_prepare_thread = Thread(target=self.target.db.hash(i * bucket, (i + 1) * bucket))
            source_prepare_thread.start()
            target_prepare_thread.start()
            source_prepare_thread.join()
            target_prepare_thread.join()
            # self.source.db.hash((i * bucket, (i + 1) * bucket))
            # self.target.db.hash((i * bucket, (i + 1) * bucket))
            source_hash = self.source.db.computed_hash()
            target_hash = self.target.db.computed_hash()
            eta=min(100,round(100*((i + 1) * bucket)/self.target.db.rowcount()))
            if source_hash != target_hash:
                rprint(
                    f"{i+1}/{loops} NOK [bold red]source hash:({source_hash}) (in {self.source.db.exec_duration()}ms)[/bold red] {eta}%"
                )
                rprint(
                    f"{i+1}/{loops} NOK [bold red]target hash:({target_hash}) (in {self.target.db.exec_duration()}ms)[/bold red] {eta}%"
                )
            else:
                rprint(
                    f"{i+1}/{loops} OK [bold blue]source hash:({source_hash}) (in {self.source.db.exec_duration()}ms)[/bold blue] {eta}%"
                )
                rprint(
                    f"{i+1}/{loops} OK [bold blue]target hash:({target_hash}) (in {self.target.db.exec_duration()}ms)[/bold blue] {eta}%"
                )

        # self.source.db.hash()
        # self.target.db.hash()

        # source_hash = self.source.db.computed_hash()
        # target_hash = self.target.db.computed_hash()  # self.source.bhash()
        # self.target.bhash()
        # offset 3 row fetch next 2 row only;

        LOGGER.info("source hashed: [%s]", source_hash)
        LOGGER.info("target hashed: [%s]", target_hash)

        self.source.db.close()
        self.target.db.close()


def main():
    args = parse_command_line(sys.argv[1:])
    compare = Compare.parse_raw(
        Configuration(args.loglevel).json_config(f"{os.path.dirname(__file__)}/{args.filename}")
    )


if __name__ == "__main__":
    main()
