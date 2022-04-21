import logging
import os
import time
from typing import Annotated, List, Literal, Optional, Union

import cx_Oracle
from mo_sql_parsing import parse, format
from pydantic import PrivateAttr
from rich import print as rprint

from .base import DBBase


LOGGER = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)

ORA_ROUNDTRIP = "select 1 from dual"

ORA_FNAME = "md5agg_clob_t"
ORA_MD5_FN_TYPE = f"""create or replace type md5agg_clob_t as object(
    v_md5 raw(16),
    v_clob clob,

    static function ODCIAggregateInitialize(
        sctx IN OUT md5agg_clob_t
    ) return number,

    member function ODCIAggregateIterate(
        self IN OUT md5agg_clob_t, value IN clob
      ) return number,

    member function ODCIAggregateTerminate(
    self IN OUT md5agg_clob_t, returnValue OUT VARCHAR2, flags IN number
      ) return number,

    member function ODCIAggregateMerge(
    self IN OUT md5agg_clob_t, ctx2 IN OUT md5agg_clob_t
  ) return number
 );"""

ORA_MD5_FN_IMPLEMENTATION = f"""create or replace type body md5agg_clob_t is

    static function ODCIAggregateInitialize(sctx IN OUT md5agg_clob_t)
    return number is
    begin
      --dbms_output.put_line('Init');
      sctx := md5agg_clob_t('','');
      return ODCIConst.Success;
    end;

    member function ODCIAggregateIterate(
      self IN OUT md5agg_clob_t,
      value IN clob
    ) return number is
    begin
      dbms_output.put_line('Iterating v_md5=['||self.v_md5||'],value=['||value||']');
      if length(value) > 0 then
          dbms_lob.createtemporary(self.v_clob, true, dbms_lob.call);
          if length(self.v_md5) > 0 then
            dbms_lob.writeappend(self.v_clob, length(self.v_md5), self.v_md5);
          end if;
          dbms_lob.writeappend(self.v_clob, length(regexp_replace(value,'(\w*\s[^,)]*)','"\1"')),regexp_replace(value,'(\w*\s[^,)]*)','"\1"'));
          dbms_output.put_line('Iterated v_clob=['||self.v_clob||']');
          self.v_md5 := dbms_crypto.hash(self.v_clob,dbms_crypto.hash_md5);
      end if;

      return ODCIConst.Success;
    end;

    member function ODCIAggregateTerminate(
      self IN OUT md5agg_clob_t,
      returnValue OUT VARCHAR2,
      flags IN number
    ) return number is
    begin
      --dbms_output.put_line('Terminate v_md5=['||self.v_md5||']');
      returnValue := rawtohex(self.v_md5);

      return ODCIConst.Success;
    end;

    member function ODCIAggregateMerge(self IN OUT md5agg_clob_t, ctx2 IN OUT md5agg_clob_t) return number is
    begin
      --dbms_output.put_line('Merge v_md5=['||self.v_md5||'],ctx2=['||ctx2.v_md5||']');
      self.v_md5:=dbms_crypto.hash(self.v_md5||ctx2.v_md5,dbms_crypto.hash_md5);
      return ODCIConst.Success;
    end;
end;
"""

ORA_MD5_FN = (
    f"""CREATE or replace FUNCTION smd5 (input clob) RETURN varchar2 PARALLEL_ENABLE AGGREGATE USING md5agg_clob_t;"""
)


def log_ora_exception(err):
    st = err.message
    LOGGER.error("\ora ERROR: %s", st)
    st = err.code
    LOGGER.error("code: %s", st)


class Oracle(DBBase):
    """Oracle connection params."""

    init_oracle_client: str
    type: Literal["oracle"]
    instance: Optional[str]
    service_name: Optional[str]

    _select: str = PrivateAttr()
    _parsed: str = PrivateAttr()
    _cx_string: str = PrivateAttr()
    _conn: cx_Oracle.Connection = PrivateAttr()
    _roundtrip: str = PrivateAttr(default=ORA_ROUNDTRIP)

    def execquery(self, qry: str):
        start = time.time_ns()
        resultset = None

        try:
            cur = self._conn.cursor()
            cur.execute(qry)
            resultset = cur.fetchone()
            self._exec_duration = (time.time_ns() - start) / 1000000
        except self._conn.InterfaceError as exc:
            pass
        except self._conn.DatabaseError as exc:
            (error,) = exc.args
            logging.error(f"""error executing {qry} : {error.code}""")
        except:
            logging.error("Error")
            raise
        finally:
            cur.close()

        return resultset

    def execquery_all(self, qry: str):
        start = time.time_ns()
        resultset = None

        try:
            cur = self._conn.cursor()
            cur.execute(qry)
            resultset = list(cur.fetchall())
            self._exec_duration = (time.time_ns() - start) / 1000000
        except self._conn.InterfaceError as exc:
            pass
        except self._conn.DatabaseError as exc:
            (error,) = exc.args
            logging.error(f"""error executing {qry} : {error.code}""")
        except:
            logging.error("Error")
            raise
        finally:
            cur.close()
            self._d7 = resultset

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._parsed = parse(self.qry)
        self._select = self.build_select()
        self._hash_qry = f"""select smd5('('||{self._select}||')') from {self._view_name}"""

    def prepare(self):
        sqlSetSession = f"""alter session set NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"""
        cx_Oracle.init_oracle_client(lib_dir="/Users/PIERRE-MARIE/Downloads/instantclient_19_8")
        if self.service_name:
            self._cx_string = (
                f"""{self.username}/{os.getenv(self.password)}@{self.host}:{self.port}/{self.service_name}"""
            )
        else:
            self._cx_string = f"""{self.username}/{os.getenv(self.password)}@{self.host}:{self.port}:{self.instance}"""
        try:
            self._conn = cx_Oracle.connect(self._cx_string)
            self._conn.stmtcachesize = 0
        except cx_Oracle.DatabaseError as exc:
            (err,) = exc.args
            log_ora_exception(err)
            raise

        # self.query(sqlSetSession)
        # sqlSetSession='alter session set ""_ORACLE_SCRIPT"=true'
        self.execquery(sqlSetSession)
        # self.execquery(ORA_MD5_FN_TYPE)
        # self.execquery(ORA_MD5_FN_IMPLEMENTATION)
        # self.execquery(ORA_MD5_FN)
        self.create_view()
        self._num_rows = self.rowcount()
        rprint(f"{self.type} Counting rows:{self._num_rows}")

    def create_view(self, start: int = 0, stop: int = 0):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """
        if start != 0 or stop != 0:
            sql = f"""create or replace view {self._view_name} as {self.qry} offset {start} rows fetch next {stop} rows only"""
        else:
            sql = f"""create or replace view {self._view_name} as {self.qry}"""

        self.execquery(sql)

    def build_select(self):
        tmp = self._parsed["select"]
        # case when instr(first_name,' ')>0 then '"'||first_name||'"' else ''||first_name||'' end as first_name
        #'\'||case when instr(first_name,\' \')>0 then "||first_name||" else ||first_name|| end as ||first_name||||\',\'||case when instr(employee_id,\' \')>0 then "||employee_id||" else ||employee_id|| end as ||employee_id||||\''
        try:
            if isinstance(tmp, List):
                st_field = ""
                for field in tmp:
                    cname = field["value"]
                    try:
                        cname = field["name"]
                    except:
                        pass
                    st_field = st_field + cname + "||','||"
                st = st_field.rstrip("','||")  #
            else:
                st = "'||" + tmp["value"] + "||'"
        except:
            raise ValueError(f"""Error building oracle select.""") from Exception

        return st
