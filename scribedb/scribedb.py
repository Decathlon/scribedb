import sys
import logging
import psycopg2


from pydantic_yaml import YamlModel
from command_line import parse_command_line
from typing import List, Optional

LOGGER = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


class Postgres:
    """Postgres connection params."""

    def __init__(self, cxString):
        try:
            self.conn = psycopg2.connect(cxString)
            self.conn.autocommit = True
        except Exception as err:
            LOGGER.error("\npsycopg2 ERROR: %s", err)
            self.conn = None
            raise

    def close(self):
        self.conn.close()


class Rdbms(YamlModel):
    """Rdbms connection params."""

    host: str
    port: int
    username: str
    password: str
    dbname: Optional[str]
    instance: Optional[str]

    def __init__(self):
        if self.dbname:
            db = Postgres(
                f"""postgresql://
              {self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}"""
            )


class Source(YamlModel):
    """Source dataset."""

    postgres: Optional[Rdbms]
    oracle: Optional[Rdbms]
    qry: str


class Target(YamlModel):
    """Target dataset."""

    postgres: Optional[Rdbms]
    oracle: Optional[Rdbms]
    qry: str


class Compare(YamlModel):
    """Compare dataset."""

    source: Source
    target: Target
    loglevel: str

def scribedb():
    """The infamous scribedb."""
    # pylint: disable=unnecessary-pass
    pass

def main():
    args = parse_command_line(sys.argv[1:])
    compare = Compare.parse_file(args.filename)
