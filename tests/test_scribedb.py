import os
import sqlalchemy
import cx_Oracle
from unittest import TestCase
from testcontainers.postgres import PostgresContainer
from testcontainers.oracle import OracleDbContainer


from main import Compare
from scribedb.configuration import Configuration


PATH = f"{os.path.dirname(__file__)}/yaml"
SQLPATH = f"{os.path.dirname(__file__)}/scripts"


def exec_qry(e, sql):
    try:
        qry = "create database db1"
        conn = e.connect()
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql)
    except Exception as err:
        print(err)

def prepare_test(e,url):
    exec_qry(e, "create database db2")
    exec_qry(
        e,
        f"""DROP TABLE if exists t_test cascade;
            CREATE TABLE t_test (a int, b int, c text);
            INSERT INTO t_test SELECT x, x + 10,'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' FROM generate_series(1, 50000) AS x;
            """,
    )
    e = sqlalchemy.create_engine(url.replace("db1", "db2"))
    exec_qry(
        e,
        f"""DROP TABLE if exists t_test cascade;
            CREATE TABLE t_test (a int, b int, c text);
            INSERT INTO t_test SELECT x, x + 10,'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' FROM generate_series(1, 50000) AS x;
            """,
    )


class TestScribedb(TestCase):
    """Provide unit tests for Main."""

    def test_rdbms_is_pg_and_ora(self):
        config_file = Configuration()
        filename = f"{PATH}/pg_and_ora.yaml"
        raw = config_file.json_config(filename)
        self.assertRaises(Exception, Compare.parse_raw, raw)

    def test_rdbms_is_not_pg_and_ora(self):
        config_file = Configuration()
        filename = f"{PATH}/no_pg_and_ora.yaml"
        raw = config_file.json_config(filename)
        self.assertRaises(Exception, Compare.parse_raw, raw)

    def test_is_ok_with_2_pg(self):
        config_file = Configuration()
        filename = f"{PATH}/2_pg_config.yaml"
        raw = config_file.json_config(filename)
        with PostgresContainer(port=5432, dbname="db1") as pg1:
            url = pg1.get_connection_url()
            e = sqlalchemy.create_engine(url)
            prepare_test(e,url)
            port = pg1.get_exposed_port("5432")
            raw = raw.replace("5432", port)
            # raw = raw.replace("PGPASSWORD", pg1.POSTGRES_PASSWORD)
            os.environ["PGPASSWORD"] = pg1.POSTGRES_PASSWORD
            raw = raw.replace("PGUSER", pg1.POSTGRES_USER)
            compare = Compare.parse_raw(raw)
            self.assertEqual(compare.source.db.type, "postgres")
            self.assertEqual(compare.target.db.type, "postgres")
            self.assertIsNotNone(compare.target.db.computed_hash())
            self.assertEqual(compare.target.db.computed_hash(), compare.source.db.computed_hash())
            self.assertTrue(Compare.parse_raw, raw)

    def test_is_nok_with_2_pg(self):
        config_file = Configuration()
        filename = f"{PATH}/2_pg_config.yaml"
        raw = config_file.json_config(filename)
        with PostgresContainer(port=5432, dbname="db1") as pg1:
            url = pg1.get_connection_url()
            e = sqlalchemy.create_engine(url)
            prepare_test(e,url)
            port = pg1.get_exposed_port("5432")
            raw = raw.replace("5432", port)
            self.assertRaises(Exception, Compare.parse_raw, raw)

    def test_nb_column_is_different(self):
        config_file = Configuration()
        filename = f"{PATH}/column_dont_match.yaml"
        raw = config_file.json_config(filename)
        self.assertRaises(Exception, Compare.parse_raw, raw)
