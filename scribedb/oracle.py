import logging
import os
import time
import json
from tkinter import E
from typing import Annotated, List, Literal, Optional, Union

import cx_Oracle
from sqlalchemy import create_engine
from mo_sql_parsing import parse, format
from pydantic import PrivateAttr
from rich import print as rprint

from .base import DBBase


ORA_ROUNDTRIP = "select 1 from dual"

ORA_FNAME = "md5agg_clob_t"
ORA_MD5_FN_TYPE = f"""
create or replace type {ORA_FNAME} as object(
    v_md5 raw(16),
    v_clob clob,

    static function ODCIAggregateInitialize(
        sctx IN OUT {ORA_FNAME}
    ) return number,

    member function ODCIAggregateIterate(
        self IN OUT {ORA_FNAME}, value IN clob
      ) return number,

    member function ODCIAggregateTerminate(
    self IN OUT {ORA_FNAME}, returnValue OUT VARCHAR2, flags IN number
      ) return number,

    member function ODCIAggregateMerge(
    self IN OUT {ORA_FNAME}, ctx2 IN OUT {ORA_FNAME}
  ) return number
 );
 """

ORA_MD5_FN_IMPLEMENTATION = f"""
create or replace type body {ORA_FNAME} is
    static function ODCIAggregateInitialize(sctx IN OUT {ORA_FNAME})
    return number is
    begin
      sctx := {ORA_FNAME}('','');
      return ODCIConst.Success;
    end;

    member function ODCIAggregateIterate(
      self IN OUT {ORA_FNAME},
      value IN clob
    ) return number is
    begin
      if length(value) > 0 then
          dbms_lob.createtemporary(self.v_clob, true, dbms_lob.call);
          if length(self.v_md5) > 0 then
            dbms_lob.writeappend(self.v_clob, length(self.v_md5), self.v_md5);
          end if;
          dbms_lob.writeappend(self.v_clob, length(regexp_replace(value,'(\w*\s[^,)]*)','"\1"')),regexp_replace(value,'(\w*\s[^,)]*)','"\1"'));
          self.v_md5 := dbms_crypto.hash(self.v_clob,dbms_crypto.hash_md5);
      end if;

      return ODCIConst.Success;
    end;

    member function ODCIAggregateTerminate(
      self IN OUT {ORA_FNAME},
      returnValue OUT VARCHAR2,
      flags IN number
    ) return number is
    begin
      returnValue := rawtohex(self.v_md5);
      return ODCIConst.Success;
    end;

    member function ODCIAggregateMerge(self IN OUT {ORA_FNAME}, ctx2 IN OUT {ORA_FNAME}) return number is
    begin
      self.v_md5:=dbms_crypto.hash(self.v_md5||ctx2.v_md5,dbms_crypto.hash_md5);
      return ODCIConst.Success;
    end;
end;
"""

ORA_MD5_FN = f"""
CREATE or replace FUNCTION smd5 (input clob)
RETURN varchar2 PARALLEL_ENABLE AGGREGATE USING {ORA_FNAME};
"""


class Oracle(DBBase):
    """Oracle connection params."""

    init_oracle_client: str
    type: Literal["oracle"]
    service_name: str

    _select: str = PrivateAttr()
    _parsed: str = PrivateAttr()
    _cx_string: str = PrivateAttr()
    _roundtrip: str = PrivateAttr(default=ORA_ROUNDTRIP)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._parsed = parse(self.qry)
        self._select = self.build_select()
        self._hash_qry = f"""select smd5('('||{self._select}||')') from {self._view_name}"""
        DIALECT = "oracle"
        SQL_DRIVER = "cx_oracle"
        USERNAME = self.username
        PASSWORD = os.getenv(self.password)
        HOST = self.host
        PORT = self.port
        SERVICE = self.service_name
        sqlUrl = (
            DIALECT
            + "+"
            + SQL_DRIVER
            + "://"
            + USERNAME
            + ":"
            + PASSWORD
            + "@"
            + HOST
            + ":"
            + str(PORT)
            + "/?service_name="
            + SERVICE
        )
        try:
            cx_Oracle.init_oracle_client(lib_dir=f"{self.init_oracle_client}")
        except Exception as e:
            print(e)
        try:
            _engine = create_engine(sqlUrl)
        except Exception as err:
            self.log_exception(err)
            _engine = None
            raise ValueError("create engine ora failed") from Exception
        try:
            self._conn = _engine.connect()
        except Exception:
            raise ValueError("connect pg failed") from Exception
        parsed_qry = parse(self.qry)
        try:
            parsed_qry.get("orderby")["value"]
        except Exception as e:
            raise ValueError("order by is required") from Exception

    def prepare(self):
        sqlSetSession = f"""alter session set NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"""
        # cx_Oracle.init_oracle_client(lib_dir="/Users/PIERRE-MARIE/Downloads/instantclient_19_8")

        # self.query(sqlSetSession)
        # sqlSetSession='alter session set ""_ORACLE_SCRIPT"=true'
        self.execquery(sqlSetSession)
        self.execquery(ORA_MD5_FN_TYPE)
        self.execquery(ORA_MD5_FN_IMPLEMENTATION)
        self.execquery(ORA_MD5_FN)
        self.create_view()
        self._num_rows = self.rowcount()
        rprint(f"{self.type} Counting rows:{self._num_rows}")

    def drop_objects(self):
        self.execquery(f"drop type {ORA_FNAME}")
        self.execquery(f"drop function smd5")

    def create_view(self, start: int = 0, stop: int = 0):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """
        sql = self.get_ddl_view(start, stop)
        self.execquery(sql)

    def build_select(self):
        tmp = self._parsed["select"]
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

        st = st + "||','||rnum "
        return st
