import sys
import logging
import os


from scribedb.command_line import parse_command_line
from scribedb.configuration import Configuration
from scribedb.oracle import Oracle
from scribedb.postgres import Postgres
from typing import Literal, Annotated, Union, List
from typing_extensions import Annotated
from threading import Thread
from pydantic import BaseModel, Field, PrivateAttr

from typing import Optional


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


class DBPostgres(DBBase):

    type: Literal["postgres"]
    dbname: str
    sslmode: Optional[str]

    _pg: Postgres = PrivateAttr()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._pg=Postgres(self.qry)

    def prepare(self):
        self._pg.prepare(self.host,self.port,self.username,self.password,self.dbname,self.sslmode)

    def colcount(self):
        return self._pg.colcount()

    def rowcount(self):
        return self._pg.rowcount()

    def hash(self):
        return self._pg.hash()

    def computed_hash(self):
        return self._pg.computed_hash

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
        self._ora=Oracle(self.qry)

    def prepare(self):
        self._ora.prepare(self.host,self.port,self.username,self.password,self.instance,self.service_name)

    def colcount(self):
        return self._ora.colcount()

    def rowcount(self):
        return self._ora.rowcount()

    def hash(self):
        return self._ora.hash()

    def computed_hash(self):
        return self._ora.computed_hash

    def close(self):
        return self._ora.close()


Db = Annotated[Union[DBPostgres, DBOracle], Field(discriminator="type")]


class Rdbms(BaseModel):
    """Rdbms dataset."""

    db: Db

    def colcount(self):
        return self.db.colcount()

    def rowcount(self):
        return self.db.rowcount()

    def prepare(self):
        return self.db.prepare()

    def hash(self):
        return self.db.hash()

    def computed_hash(self):
        return self.db.computed_hash

    def close(self):
        return self.db.close()

class Compare(BaseModel):
    """Compare dataset."""

    source: Rdbms
    target: Rdbms
    loglevel: str
    max_delta: Optional[int]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        source_colcount = self.source.colcount()
        target_colcount = self.target.colcount()
        if (source_colcount != target_colcount) and target_colcount > 0:
            raise ValueError(
                f"""Number of column in target [{target_colcount}] is <> from source [{source_colcount}] dataset are different."""
            ) from Exception

        # souce_thread = Thread(target=self.source.prepare())
        # target_thread = Thread(target=self.target.prepare())
        # souce_thread.start()
        # target_thread.start()
        # souce_thread.join()
        # target_thread.join()

        self.source.prepare()
        self.target.prepare()

        LOGGER.info("source hashing: %s rows", self.source.rowcount())
        LOGGER.info("target hashing: %s rows", self.target.rowcount())

        # souce_thread = Thread(target=self.source.hash())
        # target_thread = Thread(target=self.target.hash())
        # souce_thread.start()
        # target_thread.start()
        # souce_thread.join()
        # target_thread.join()

        source_hash=self.source.hash()
        target_hash=self.target.hash()


        LOGGER.info("source hashed: [%s]", source_hash)
        LOGGER.info("target hashed: [%s]", target_hash)

        self.source.close()
        self.target.close()


def main():
    args = parse_command_line(sys.argv[1:])
    compare = Compare.parse_raw(
        Configuration(args.loglevel).json_config(f"{os.path.dirname(__file__)}/{args.filename}")
    )


if __name__ == "__main__":
    main()
