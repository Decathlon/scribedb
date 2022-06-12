import os
import sqlalchemy
import cx_Oracle
import json
from unittest import TestCase
import pytest
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


os.environ["PGPASSWORD"] = "test"
os.environ["ORAPASSWORD"] = "oracle"


def prepare_test(e, url):
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


def set_grants_ora(url, username):
    url_sysdba = url.replace("system", "sys")
    mode = cx_Oracle.SYSDBA
    url_sysdba = f"{url_sysdba}?mode={mode}"
    try:
        e = sqlalchemy.create_engine(url_sysdba)
    except Exception as e:
        print(e)
    e.execute(f"grant execute on dbms_crypto to {username}")


def prepare_test_ora(e, size=5000):
    e.execute(f"CREATE OR REPLACE TYPE numbers_t AS TABLE OF NUMBER")
    e.execute(
        f"""CREATE OR REPLACE FUNCTION generate_series (minnumber INTEGER, maxnumber INTEGER)
    RETURN numbers_t
    PIPELINED
    DETERMINISTIC
        IS
        BEGIN
        FOR i IN minnumber .. maxnumber LOOP
            PIPE ROW (i);
        END LOOP;
        RETURN;
        END;
        """
    )
    try:
        e.execute("DROP TABLE t_test")
    except:
        pass
    e.execute("CREATE TABLE t_test (a number, b number, c varchar2(255))")
    e.execute(
        f"INSERT INTO t_test SELECT column_value, column_value + 10,'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' FROM table(generate_series(1, {size}))"
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
            prepare_test(e, url)
            port = pg1.get_exposed_port("5432")
            raw = raw.replace("5432", port)
            compare = Compare.parse_raw(raw)
            self.assertEqual(compare.source.db.type, "postgres")
            self.assertEqual(compare.target.db.type, "postgres")
            self.assertIsNotNone(compare.target.db.computed_hash())
            self.assertEqual(compare.target.db.computed_hash(), compare.source.db.computed_hash())
            self.assertTrue(Compare.parse_raw, raw)

    @pytest.mark.skip(reason="needs oracle client libraries unavailable on Travis")
    def test_is_ok_with_2_ora(self):
        config_file = Configuration()
        filename = f"{PATH}/2_ora_config.yaml"
        raw = config_file.json_config(filename)
        parsed_raw = json.loads(raw)
        cx_Oracle.init_oracle_client(lib_dir=f"/Users/PIERRE-MARIE/Downloads/instantclient_19_8")

        with OracleDbContainer() as oracle_source:
            with OracleDbContainer() as oracle_target:
                url = oracle_source.get_connection_url()
                e = sqlalchemy.create_engine(url)
                set_grants_ora(url, "system")
                prepare_test_ora(e, 50000)
                url = oracle_target.get_connection_url()
                e = sqlalchemy.create_engine(url)
                set_grants_ora(url, "system")
                prepare_test_ora(e, 50000)

                result = e.execute("select count(*) from t_test")
                for row in result:
                    self.assertEqual(row[0], 50000)

                parsed_raw["source"]["db"]["port"] = oracle_source.get_exposed_port("1521")
                parsed_raw["target"]["db"]["port"] = oracle_target.get_exposed_port("1521")

                raw = json.dumps(parsed_raw)

                compare = Compare.parse_raw(raw)
                self.assertEqual(compare.source.db.type, "oracle")
                self.assertEqual(compare.target.db.type, "oracle")
                self.assertIsNotNone(compare.target.db.computed_hash())
                self.assertEqual(compare.target.db.computed_hash(), compare.source.db.computed_hash())

    def test_is_nok_with_2_ora(self):
        config_file = Configuration()
        filename = f"{PATH}/2_ora_config.yaml"
        raw = config_file.json_config(filename)
        parsed_raw = json.loads(raw)
        cx_Oracle.init_oracle_client(lib_dir=f"/Users/PIERRE-MARIE/Downloads/instantclient_19_8")
        with OracleDbContainer() as oracle_source:
            with OracleDbContainer() as oracle_target:
                url = oracle_source.get_connection_url()
                e = sqlalchemy.create_engine(url)
                set_grants_ora(url, "system")
                prepare_test_ora(e, 50000)
                url = oracle_target.get_connection_url()
                e = sqlalchemy.create_engine(url)
                set_grants_ora(url, "system")
                prepare_test_ora(e, 50000)

                e.execute("update system.t_test set c='abcdefghijklmnopqrstuvwxy' where a=10")

                parsed_raw["source"]["db"]["port"] = oracle_source.get_exposed_port("1521")
                parsed_raw["target"]["db"]["port"] = oracle_target.get_exposed_port("1521")

                raw = json.dumps(parsed_raw)
                self.assertRaises(ValueError, Compare.parse_raw, raw)

    def test_is_nok_with_2_pg(self):
        config_file = Configuration()
        filename = f"{PATH}/2_pg_config.yaml"
        raw = config_file.json_config(filename)
        with PostgresContainer(port=5432, dbname="db1") as pg1:
            url = pg1.get_connection_url()
            e = sqlalchemy.create_engine(url)
            prepare_test(e, url)
            e.execute("update t_test set c='abcdefghijklmnopqrstuvwxy' where a=10")
            port = pg1.get_exposed_port("5432")
            raw = raw.replace("5432", port)
            self.assertRaises(ValueError, Compare.parse_raw, raw)

    def test_nb_column_is_different(self):
        config_file = Configuration()
        filename = f"{PATH}/column_dont_match.yaml"
        raw = config_file.json_config(filename)
        self.assertRaises(ValueError, Compare.parse_raw, raw)

    def test_no_order_by(self):
        config_file = Configuration()
        filename = f"{PATH}/no_order_by.yaml"
        raw = config_file.json_config(filename)
        with PostgresContainer(port=5432, dbname="db1") as pg1:
            url = pg1.get_connection_url()
            e = sqlalchemy.create_engine(url)
            prepare_test(e, url)
            port = pg1.get_exposed_port("5432")
            raw = raw.replace("5432", port)
            self.assertRaises(ValueError, Compare.parse_raw, raw)
