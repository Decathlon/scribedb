import logging
import random

import cx_Oracle


from mo_sql_parsing import parse
from typing import List

LOGGER = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


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
      dbms_output.put_line('Init');
      sctx := md5agg_clob_t('','');
      return ODCIConst.Success;
    end;

    member function ODCIAggregateIterate(
      self IN OUT md5agg_clob_t,
      value IN clob
    ) return number is
    begin
      dbms_output.put_line('Iterate v_md5=['||self.v_md5||'],value=['||value||']');
      if length(value) > 0 then
          dbms_lob.createtemporary(self.v_clob, true, dbms_lob.call);
          if length(self.v_md5) > 0 then
            dbms_lob.writeappend(self.v_clob, length(self.v_md5), self.v_md5);
          end if;
          if instr(value,' ') = 0 then
            dbms_lob.writeappend(self.v_clob, length(value), value);
          else
            dbms_lob.writeappend(self.v_clob, length(value)+2, '("'||ltrim(rtrim(value,')'),'(')||'")');
          end if;
          dbms_output.put_line('Iterate v_clob=['||self.v_clob||']');
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
      dbms_output.put_line('Terminate v_md5=['||self.v_md5||']');
      returnValue := rawtohex(self.v_md5);

      return ODCIConst.Success;
    end;

    member function ODCIAggregateMerge(self IN OUT md5agg_clob_t, ctx2 IN OUT md5agg_clob_t) return number is
    begin
      dbms_output.put_line('Merge v_md5=['||self.v_md5||'],ctx2=['||ctx2.v_md5||']');
      self.v_md5:=dbms_crypto.hash(self.v_md5||ctx2.v_md5,dbms_crypto.hash_md5);
      return ODCIConst.Success;
    end;
end;
"""

ORA_MD5_FN = (
    f"""CREATE or replace FUNCTION smd5 (input clob) RETURN varchar2 PARALLEL_ENABLE AGGREGATE USING md5agg_clob_t;"""
)


PREFIX = "scdb_"
ASCII_LETTER = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def random_char(y):
    tmp = "".join(random.choice(ASCII_LETTER) for x in range(y))
    return tmp


def log_ora_exception(err):
    st = err.message
    LOGGER.error("\ora ERROR: %s", st)
    st = err.code
    LOGGER.error("code: %s", st)


class Oracle:
    """Oracle connection params."""

    LOGGER.setLevel(getattr(logging, "INFO"))

    view_name = random_char(20)
    computed_hash: str

    def execquery(self, qry: str):
        resultset = None
        try:
            cur = self.conn.cursor()
            cur.execute(qry)
            resultset = cur.fetchall()
            cur.close()
        except self.conn.InterfaceError as exc:
            pass
        except self.conn.DatabaseError as exc:
            (error,) = exc.args
            logging.error(f"""{self.dbEngine}:error executing {qry} : {error.code}""")
        return resultset

    def __init__(self, qry):
        self._fields = parse(qry)
        self.qry = qry
        self._select = self.build_select()

    def prepare(self, host, port, username, password, instance, service_name):
        sqlSetSession = f"""alter session set NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"""
        cx_Oracle.init_oracle_client(lib_dir="/Users/PIERRE-MARIE/Downloads/instantclient_19_8")
        if service_name:
            cxString = f"""{username}/{password}@{host}:{port}/{service_name}"""
        else:
            cxString = f"""{username}/{password}@{host}:{port}:{instance}"""
        try:
            self.conn = cx_Oracle.connect(cxString)
        except cx_Oracle.DatabaseError as exc:
            (err,) = exc.args
            log_ora_exception(err)
            raise

        # self.query(sqlSetSession)
        # sqlSetSession='alter session set ""_ORACLE_SCRIPT"=true'
        self.execquery(sqlSetSession)
        self.execquery(ORA_MD5_FN_TYPE)
        self.execquery(ORA_MD5_FN_IMPLEMENTATION)
        self.execquery(ORA_MD5_FN)
        self.create_view(self.qry)

    def drop_md5_fn(self):
        self.execquery("fdrop function {FNAME} cascade")

    def create_view(self, query):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """

        sql = f"""create view {self.view_name} as {query}"""
        self.execquery(sql)

    def build_select(self):
        tmp = self._fields["select"]
        if isinstance(tmp, List):
            st_field = ""
            for field in tmp:
                f = field["value"]
                st_field = st_field + "'||" + f + "||',"
        else:
            st_field = "'||" + tmp["value"] + "||',"
        st = st_field.rstrip(",")
        return st

    def hash(self):
        sql = f"""select smd5('({self._select})') from {self.view_name}"""
        tmp = self.execquery(sql)
        self.computed_hash = tmp[0][0]
        return self.computed_hash

    def drop_view(self):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """

        sql = f"""drop view {self.view_name}"""
        self.execquery(sql)

    def colcount(self):
        return len(self._fields["select"])

    def rowcount(self):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """

        sql = f"""select count(*) from {self.view_name}"""
        ret = self.execquery(sql)
        return ret[0][0]

    def close(self):
        self.drop_view()
        self.conn.close()
