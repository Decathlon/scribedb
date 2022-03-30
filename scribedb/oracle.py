import logging
import time
import cx_Oracle
from rich import print as rprint


from mo_sql_parsing import parse
from typing import List

ESTIMATE_LOOP = 2
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


class Oracle:
    """Oracle connection params."""

    LOGGER.setLevel(getattr(logging, "INFO"))

    def execquery(self, qry: str):
        start = time.time_ns()
        resultset = None

        try:
            cur = self.conn.cursor()
            cur.execute(qry)
            resultset = cur.fetchone()
            self.exec_duration = (time.time_ns() - start) / 1000000
        except self.conn.InterfaceError as exc:
            pass
        except self.conn.DatabaseError as exc:
            (error,) = exc.args
            logging.error(f"""{self.dbEngine}:error executing {qry} : {error.code}""")
        except:
            logging.error("Error")
            raise
        finally:
            cur.close()

        return resultset

    def __init__(self, qry, view_name):
        self._parsed = parse(qry)
        self.qry = qry
        self._select = self.build_select()
        self.view_name = view_name
        self.bucket = 0

    def prepare(self, host, port, username, password, instance, service_name):
        sqlSetSession = f"""alter session set NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"""
        cx_Oracle.init_oracle_client(lib_dir="/Users/PIERRE-MARIE/Downloads/instantclient_19_8")
        if service_name:
            self.cxString = f"""{username}/{password}@{host}:{port}/{service_name}"""
        else:
            self.cxString = f"""{username}/{password}@{host}:{port}:{instance}"""
        try:
            self.conn = cx_Oracle.connect(self.cxString)
            self.conn.stmtcachesize = 0
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
        self.num_rows = self.rowcount()

    def drop_md5_fn(self):
        self.execquery("fdrop function {FNAME} cascade")

    def create_view(self, start: int = 0, stop: int = 0):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """
        if start != 0 or stop != 0:
            sql = f"""create or replace view {self.view_name} as {self.qry} offset {start} rows fetch next {stop} rows only"""
        else:
            sql = f"""create or replace view {self.view_name} as {self.qry}"""

        self.execquery(sql)

    def hash(self, start: int = 0, stop: int = 0):
        self.create_view(start, stop)
        sql = f"""select smd5('('||{self._select}||')') from {self.view_name}"""
        tmp = self.execquery(sql)
        self._computed_hash = tmp[0]

    def explain(self, qry):
        ret = 0
        st = self.execquery(qry)
        if st[0] != None:
            ret = self.exec_duration
        else:
            raise ValueError(f"""Returned value is null.""") from Exception
        return ret

    def estimate_execution(self, qry_exec_time):
        """time = a.rows + round_trip
        rows = (time - (round_trip))/a
        """

        round_trip = 0
        time = 0
        rows = 0
        for i in range(ESTIMATE_LOOP):
            rprint(f"Ora: Estimating round_trip")
            round_trip = round_trip + self.explain(ORA_ROUNDTRIP)
        round_trip = round_trip / ESTIMATE_LOOP
        for j in range(ESTIMATE_LOOP):
            r = (j + 1) + j * 100 - 2 * j
            self.create_view(0, r)
            for i in range(ESTIMATE_LOOP):
                sql = f"""select smd5('('||{self._select}||')') from {self.view_name}"""
                rprint(f"Ora: Estimating for {r} rows")
                time = time + self.explain(sql)
                rows = rows + r

        tx = time / rows
        bucket = round((qry_exec_time - round_trip) / tx)

        # LOGGER.info(f"""Estimation of [{bucket}] rows could be computed in [{qry_exec_time}ms]""")

        self.create_view(0, bucket)
        sql = f"""select smd5('('||{self._select}||')') from {self.view_name}"""
        runtime = self.explain(sql)

        while runtime < (qry_exec_time - 20 * qry_exec_time / 100):
            if bucket >= self.num_rows // 2:
                break
            bucket = round(bucket * 2)
            self.create_view(0, bucket)
            sql = f"""select smd5('('||{self._select}||')') from {self.view_name}"""
            runtime = self.explain(sql)

        #    LOGGER.info(f"""Effective [{bucket}] rows are computed in [{runtime}ms]""")

        self.bucket = bucket

    def computed_hash(self):
        return self._computed_hash

    def drop_view(self):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """

        sql = f"""drop view {self.view_name}"""
        self.execquery(sql)

    def build_select(self):
        tmp = self._parsed["select"]
        # case when instr(first_name,' ')>0 then '"'||first_name||'"' else ''||first_name||'' end as first_name
        #'\'||case when instr(first_name,\' \')>0 then "||first_name||" else ||first_name|| end as ||first_name||||\',\'||case when instr(employee_id,\' \')>0 then "||employee_id||" else ||employee_id|| end as ||employee_id||||\''
        if isinstance(tmp, List):
            st_field = ""
            for field in tmp:
                st_field = st_field + field["value"] + "||','||"
            st = st_field.rstrip("','||")  #
        else:
            st = "'||" + tmp["value"] + "||'"

        return st

    def rowcount(self):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """

        sql = f"""select count(*) from {self.view_name}"""
        ret = self.execquery(sql)
        return ret[0]

    def close(self):
        self.drop_view()
        self.conn.close()
