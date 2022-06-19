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

class Rdbms(BaseModel):
    """Rdbms dataset."""

    db: Db
    name: str

def compare_d7(source: Rdbms, target: Rdbms):
    def print_d7(rows):
        for i in range(len(rows)):
            row=tuple({rows.pop()})
            rprint(f"{row[0]} qry:{source.db.get_select(row[0][len(row[0])-1])}")
            errors = +1
        return errors

    target.db.retreive_dataset()
    source.db.retreive_dataset()
    target_ds = Dataset(target.db.get_dataset(), target.name)
    source_ds = Dataset(source.db.get_dataset(), source.name)

    errors = 0
    if source_ds.d7 is not None:
        sources1 = set(source_ds.d7)
        if target_ds.d7 is not None:
            targets2 = set(target_ds.d7)
            results = set.symmetric_difference(sources1, targets2)
            if len(results) > 0:
                err = print_d7(sorted(results))
                errors = +err
    return errors


class Compare(BaseModel):
    """Compare dataset."""

    source: Rdbms
    target: Rdbms
    loglevel: str
    max_delta: Optional[int] = 10

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
            f"{self.source.name} can hash ({self.source.db.get_bucket()}) rows in {QRY_EXECUTION_TIME} ms. Total rows:{self.source.db.get_d7_num_rows()}"
        )
        rprint(
            f"{self.target.name} can hash ({self.target.db.get_bucket()}) rows in {QRY_EXECUTION_TIME} ms. Total rows:{self.target.db.get_d7_num_rows()}"
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
            source_prepare_thread = Thread(target=self.source.db.hash(i * bucket, bucket))
            target_prepare_thread = Thread(target=self.target.db.hash(i * bucket, bucket))
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
                    f"{i+1}/{loops} NOK [bold red]{self.source.name} hash:({source_hash}) (start: {i * bucket} limit:{(i + 1) * bucket} rows computed:{self.source.db.computed_rows()} in {self.source.db.get_exec_duration()} ms)[/bold red] {eta}%"
                )
                rprint(
                    f"{i+1}/{loops} NOK [bold red]{self.target.name} hash:({target_hash}) (start: {i * bucket} limit:{(i + 1) * bucket} rows computed:{self.target.db.computed_rows()} in {self.target.db.get_exec_duration()} ms)[/bold red] {eta}%"
                )
                err = compare_d7(self.source, self.target)
                _errors = +err
            else:
                rprint(
                    f"{i+1}/{loops} OK [bold blue]{self.source.name} hash:({source_hash}) (start: {i * bucket} limit:{bucket} rows computed:{self.source.db.computed_rows()} in {self.source.db.get_exec_duration()} ms)[/bold blue] {eta}%"
                )
                rprint(
                    f"{i+1}/{loops} OK [bold blue]{self.target.name} hash:({target_hash}) (start: {i * bucket} limit:{bucket} rows computed:{self.target.db.computed_rows()} in {self.target.db.get_exec_duration()} ms)[/bold blue] {eta}%"
                )
            if _errors>=self.max_delta:
                break

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
