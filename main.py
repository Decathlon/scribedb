import logging
import math
import os
import random
import sys
from threading import Thread
from typing import Annotated, List, Literal, Optional, Union

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


# class DBPostgres(DBBase):

#     type: Literal["postgres"]
#     dbname: str
#     sslmode: Optional[str]

#     _pg: Postgres = PrivateAttr()

#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self._pg = Postgres(self.qry, self._view_name)

#     def prepare(self):
#         self._pg.prepare(self.host, self.port, self.username, os.getenv(self.password), self.dbname, self.sslmode)

#     def rowcount(self):
#         return self._pg.num_rows

#     def hash(self, start:int=0, stop:int=0):
#         return self._pg.hash(start, stop)

#     def computed_hash(self):
#         return self._pg.computed_hash()

#     def estimate(self):
#         self._pg.estimate_execution(self._qry_exec_time)

#     def exec_duration(self):
#         return round(self._pg.exec_duration,1)

#     def get_bucket(self):
#         return self._pg.bucket

#     def close(self):
#         return self._pg.close()


# class DBOracle(DBBase):

#     init_oracle_client: str
#     type: Literal["oracle"]
#     instance: Optional[str]
#     service_name: Optional[str]

#     _ora: Oracle = PrivateAttr()

#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self._ora = Oracle(self.qry, self._view_name)

#     def prepare(self):
#         self._ora.prepare(
#             self.host, self.port, self.username, os.getenv(self.password), self.instance, self.service_name
#         )

#     def rowcount(self):
#         return self._ora.num_rows

#     def hash(self, start:int=0, stop:int=0):
#         return self._ora.hash(start, stop)

#     def computed_hash(self):
#         return self._ora.computed_hash()

#     def estimate(self):
#         self._ora.estimate_execution(self._qry_exec_time)

#     def exec_duration(self):
#         return round(self._ora.exec_duration,1)

#     def get_bucket(self):
#         return self._ora.bucket

#     def close(self):
#         return self._ora.close()


Db = Annotated[Union[Postgres, Oracle], Field(discriminator="type")]


class Dataset:
    d7: list
    name: str

    def __init__(self, d7, name):
        self.d7 = d7
        self.name = name


def compare_d7(rows1: Dataset, rows2: Dataset):
    def print_d7(rows, name):
        i_err: int = 0
        for i in range(len(rows)):
            rprint(f"{name}:{rows.pop()}")
            i_err = +1
        return i_err

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

    _errors: int = PrivateAttr(default=0)

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

        # source_hash_thread = Thread(target=self.source.db.hash())
        # target_hash_thread = Thread(target=self.target.db.hash())
        # source_hash_thread.start()
        # target_hash_thread.start()
        # source_hash_thread.join()
        # target_hash_thread.join()

        bucket = min(self.source.db.get_bucket(), self.target.db.get_bucket())
        rows = max(self.source.db.get_d7_num_rows(), self.target.db.get_d7_num_rows())
        loops = math.ceil(rows / bucket)
        if loops == 0:
            rprint(
                f"[red]Dataset are empty source:({self.source.db.get_d7_num_rows()}) target:(${self.target.db.get_d7_num_rows()}"
            )
            exit()
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
            raise ValueError(f"""Datasets are different.""") from Exception
        else:
            rprint("[bold blue]Datasets are identicals")


def main():
    args = parse_command_line(sys.argv[1:])
    compare = Compare.parse_raw(
        Configuration(args.loglevel).json_config(f"{os.path.dirname(__file__)}/{args.filename}")
    )


if __name__ == "__main__":
    main()


#    qry: SELECT id, store, case when item=0 then null else item end as item,nature, delta,
#      type, "user",date FROM movements.stock_movements where store in '||:listemag||'
#      and date between date_trunc('hour',now() at time zone 'utc' - interval '30 hours')
#      and date_trunc('hour',now() at time zone 'utc' - interval '2 hour') order by id



    # qry: with cte as (select
    #   a.msk_id,
    #   a.tir_num_tiers_tir,
    #   a.tir_sous_num_tiers_tir,
    #   a.tti_num_type_tiers_tir,
    #   to_number(FST_ARTICLE_R3) FST_ARTICLE_R3,
    #   a.nmv_code_nature_mouvement_nmv,
    #   a.msk_quantite*NMV_SIGNE_IMPACT_STOCK as delta,
    #   a.tys_type_stock_tys,
    #   a.msk_user,
    #   a.msk_date_ecriture,
    #   case
    #   when lower(tz.tzo_timezone_id_reel) = 'america/sao_paulo' then '-03:00'
    #   when lower(tz.tzo_timezone_id_reel) = 'africa/cairo' then '+02:00'
    #   when lower(tz.tzo_timezone_id_reel) = 'europe/moscow' then '+03:00'
    #   when lower(tz.tzo_timezone_id_reel) = 'europe/istanbul' then '+03:00'
    #   when lower(tz.tzo_timezone_id_reel) = 'africa/casablanca' and a.msk_date_ecriture between to_date('11/04/2021 03:00:00','DD/MM/YYYY HH24:MI:SS') and to_date('16/05/2021 02:00:00','DD/MM/YYYY HH24:MI:SS')  then '+00:00'
    #   when lower(tz.tzo_timezone_id_reel) = 'africa/casablanca' and a.msk_date_ecriture between to_date('27/03/2022 03:00:00','DD/MM/YYYY HH24:MI:SS') and to_date('08/05/2022 02:00:00','DD/MM/YYYY HH24:MI:SS')  then '+00:00'
    #   when lower(tz.tzo_timezone_id_reel) = 'africa/casablanca' then '+01:00'
    #   when lower(tz.tzo_timezone_id_reel) = 'america/santiago' and a.msk_date_ecriture between to_date('04/04/2021 00:00:00','DD/MM/YYYY HH24:MI:SS') and to_date('05/09/2021 00:00:00','DD/MM/YYYY HH24:MI:SS')  then '-04:00'
    #   when lower(tz.tzo_timezone_id_reel) = 'america/santiago' and a.msk_date_ecriture between to_date('03/04/2022 00:00:00','DD/MM/YYYY HH24:MI:SS') and to_date('04/09/2022 00:00:00','DD/MM/YYYY HH24:MI:SS')  then '-04:00'
    #   when lower(tz.tzo_timezone_id_reel) = 'america/santiago' then '-03:00'
    #   else tz.tzo_timezone_id_reel
    #   end as tz_fixed
    #   from stcom.mouvement_stock a
    #   inner join masterdatas.flat_structure b on
    #   a.elg_num_elt_gestion_elg = b.elg_num_elt_gestion_elg
    #   inner join stcom.nature_mouvement c on c.nmv_code_nature_mouvement = a.nmv_code_nature_mouvement_nmv
    #   inner join masterdatas.element_gestion eg on a.elg_num_elt_gestion_elg=eg.elg_num_elt_gestion
    #   inner join STCOM.timezone_tiers tz on tz.tir_num_tiers_tir=a.tir_num_tiers_tir and
    #   tz.tir_sous_num_tiers_tir=a.tir_sous_num_tiers_tir and
    #   tz.tti_num_type_tiers_tir=a.tti_num_type_tiers_tir
    #   where msk_date_ecriture>=trunc(sysdate-2*30/24)  and msk_flag_stock IN (1, 2) and eg.nat_num_nature_nat=2)
    #   select msk_id,
    #   cte.tir_num_tiers_tir,
    #   FST_ARTICLE_R3,
    #   nmv_code_nature_mouvement_nmv,
    #   delta,
    #   tys_type_stock_tys,
    #   msk_user,
    #   sys_extract_utc(from_tz(cast(MSK_DATE_ECRITURE as timestamp),cte.tz_fixed)) as msk_date_ecriture
    #   from cte cte
    #   where sys_extract_utc(from_tz(cast(MSK_DATE_ECRITURE as timestamp),cte.tz_fixed)) between trunc(sys_extract_utc(systimestamp)-30/24,'HH') and trunc(sys_extract_utc(systimestamp)-2/24,'HH') order by msk_id
