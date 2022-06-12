import logging
import math
import os
import random
import sys
from threading import Thread
from typing import Annotated, List, Literal, Optional, Union

from mo_sql_parsing import parse
from pydantic import BaseModel, Field, PrivateAttr
from rich import print as rprint
from typing_extensions import Annotated

from scribedb.command_line import parse_command_line
from scribedb.configuration import Configuration
from scribedb.oracle import Oracle
from scribedb.postgres import Postgres

QRY_EXECUTION_TIME = 5000

LOGGER = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


Db = Annotated[Union[Postgres, Oracle], Field(discriminator="type")]

class Dataset:
    d7: list
    name: str

    def __init__(self, d7, name):
        self.d7 = d7
        self.name = name


def compare_d7(rows1: Dataset, rows2: Dataset):
    def print_d7(rows, name):
        for i in range(len(rows)):
            rprint(f"{name}:{rows.pop()}")
            errors = +1
        return errors

    errors = 0
    if rows1.d7 is not None:
        rowsets1 = set(rows1.d7)
        if rows2.d7 is not None:
            rowsets2 = set(rows2.d7)
            result_a_b = rowsets1 - rowsets2
            result_b_a = rowsets2 - rowsets1
            if len(result_a_b) > 0:
                err = print_d7(result_a_b, rows1.name)
                errors = +err
            if len(result_b_a) > 0:
                err = print_d7(result_b_a, rows2.name)
                errors = +err
    return errors


class Rdbms(BaseModel):
    """Rdbms dataset."""

    db: Db
    name: str


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
        _errors: int = 0

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

        source_prepare_thread = Thread(target=self.source.db.estimate_bucket_size(QRY_EXECUTION_TIME))
        target_prepare_thread = Thread(target=self.target.db.estimate_bucket_size(QRY_EXECUTION_TIME))
        source_prepare_thread.start()
        target_prepare_thread.start()
        source_prepare_thread.join()
        target_prepare_thread.join()

        # self.source.db.estimate_bucket_size(QRY_EXECUTION_TIME)
        # self.target.db.estimate_bucket_size(QRY_EXECUTION_TIME)

        # LOGGER.info("source hashing: %s rows", self.source.db.rowcount())
        # LOGGER.info("target hashing: %s rows", self.target.db.rowcount())
        rprint(
            f"{self.source.name} can hash ({self.source.db.get_bucket()}) rows in {QRY_EXECUTION_TIME}ms num_rows:{self.source.db.get_d7_num_rows()}"
        )
        rprint(
            f"{self.target.name} can hash ({self.target.db.get_bucket()}) rows in {QRY_EXECUTION_TIME}ms num_rows:{self.target.db.get_d7_num_rows()}"
        )

        bucket = min(self.source.db.get_bucket(), self.target.db.get_bucket())
        rows = max(self.source.db.get_d7_num_rows(), self.target.db.get_d7_num_rows())
        loops = math.ceil(rows / bucket)
        if loops == 0:
            rprint(
                f"[red]Dataset are empty source:({self.source.db.get_d7_num_rows()}) target:(${self.target.db.get_d7_num_rows()}"
            )
            exit
        est_time = loops * QRY_EXECUTION_TIME // 1000
        rprint(f"Total estimated time: [{est_time}]s")
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
            eta = min(100, round(100 * ((i + 1) * bucket) / rows))
            if source_hash != target_hash:
                rprint(
                    f"{i+1}/{loops} NOK [bold red]{self.source.name} hash:({source_hash}) (in {self.source.db.get_exec_duration()}ms)[/bold red] {eta}%"
                )
                rprint(
                    f"{i+1}/{loops} NOK [bold red]{self.target.name} hash:({target_hash}) (in {self.target.db.get_exec_duration()}ms)[/bold red] {eta}%"
                )
                self.target.db.retreive_dataset()
                self.source.db.retreive_dataset()
                target_ds = Dataset(self.target.db.get_dataset(), self.target.name)
                source_ds = Dataset(self.source.db.get_dataset(), self.source.name)
                err = compare_d7(source_ds, target_ds)
                _errors = +err
            else:
                rprint(
                    f"{i+1}/{loops} OK [bold blue]{self.source.name} hash:({source_hash}) (in {self.source.db.get_exec_duration()}ms)[/bold blue] {eta}%"
                )
                rprint(
                    f"{i+1}/{loops} OK [bold blue]{self.target.name} hash:({target_hash}) (in {self.target.db.get_exec_duration()}ms)[/bold blue] {eta}%"
                )

        # self.source.db.hash()
        # self.target.db.hash()

        # source_hash = self.source.db.computed_hash()
        # target_hash = self.target.db.computed_hash()  # self.source.bhash()
        # self.target.bhash()
        # offset 3 row fetch next 2 row only;

        # LOGGER.info("source hashed: [%s]", source_hash)
        # LOGGER.info("target hashed: [%s]", target_hash)

        self.source.db.close()
        self.target.db.close()

        if _errors != 0:
            rprint("[bold red]Datasets are different")
            #raise NameError('Datasets are different.')
            raise ValueError("Datasets are different") from Exception
            #raise Exception(f"""Datasets are different.""") from Exception
        else:
            rprint("[bold blue]Datasets are identicals")


def main():
    args = parse_command_line(sys.argv[1:])
    compare = Compare.parse_raw(
        Configuration(args.loglevel).json_config(f"{os.path.dirname(__file__)}/{args.filename}")
    )


if __name__ == "__main__":
    main()
