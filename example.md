## Description

the example below assumes that 2 databases are running:

* Oracle
* Postgresql


Scribedb will compare data in 2 tables between postgresql and oracle.

## Running the example

### oracle setup


the test will be run with system user.
as sysdba you must grant execute

```sql
sqlplus / as sysdba
grant execute on dbms_crypto to system;
```
#### create oracle aggregate function

created during the execution. It is just for information

```sql
CREATE OR REPLACE NONEDITIONABLE TYPE "MD5AGG_CLOB_T_STR" as object(
    v_md5 raw(16),
    v_clob varchar2(4000),

    static function ODCIAggregateInitialize(
        sctx IN OUT md5agg_clob_t_str
    ) return number,

    member function ODCIAggregateIterate(
        self IN OUT md5agg_clob_t_str, value IN varchar2
      ) return number,

    member function ODCIAggregateTerminate(
    self IN OUT md5agg_clob_t_str, returnValue OUT VARCHAR2, flags IN number
      ) return number,

    member function ODCIAggregateMerge(
    self IN OUT md5agg_clob_t_str, ctx2 IN OUT md5agg_clob_t_str
  ) return number
 );
/

CREATE OR REPLACE NONEDITIONABLE TYPE BODY "MD5AGG_CLOB_T_STR" is
    static function ODCIAggregateInitialize(sctx IN OUT md5agg_clob_t_str)
    return number is
    begin
      sctx := md5agg_clob_t_str('','');
      return ODCIConst.Success;
    end;

    member function ODCIAggregateIterate(
      self IN OUT md5agg_clob_t_str,
      value IN varchar2
    ) return number is
    begin
        self.v_md5 := dbms_crypto.hash(to_clob(self.v_md5||regexp_replace(value,'(\w*\s[^,)]*)','"\1"')),dbms_crypto.hash_md5);
        return ODCIConst.Success;
    end;

    member function ODCIAggregateTerminate(
      self IN OUT md5agg_clob_t_str,
      returnValue OUT VARCHAR2,
      flags IN number
    ) return number is
    begin
      returnValue := rawtohex(self.v_md5);

      return ODCIConst.Success;
    end;

    member function ODCIAggregateMerge(self IN OUT md5agg_clob_t_str, ctx2 IN OUT md5agg_clob_t_str) return number is
    begin
      self.v_md5:=dbms_crypto.hash(to_clob(self.v_md5||ctx2.v_md5),dbms_crypto.hash_md5);
      return ODCIConst.Success;
    end;
end;
/

CREATE or replace FUNCTION smd5 (input varchar2) RETURN varchar2 PARALLEL_ENABLE AGGREGATE USING md5agg_clob_t_str;
/

```
#### create oracle testing table

We will create a table with some data provided by a deterministic function.

The generate function is:

```sql
CREATE OR REPLACE TYPE numbers_t AS TABLE OF NUMBER;
/

CREATE OR REPLACE FUNCTION generate_series (minnumber INTEGER, maxnumber INTEGER)
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
/
```

The table we will use for this test: 

```sql
DROP TABLE t_test;
CREATE TABLE t_test (a number, b number, c varchar2(255));
INSERT INTO t_test SELECT column_value, column_value + 10,'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' FROM generate_series(1, 50000);

commit;
```

### postgresql setup

#### create pg aggregate function

Created during execution. It is just for information.

```sql
CREATE or replace FUNCTION md5_agg_sfunc_t(text, text)
RETURNS text
LANGUAGE sql
AS
$$
  SELECT upper(md5($1 || $2::text))
$$;

CREATE or replace AGGREGATE md5_agg_t (ORDER BY anyelement)
(
  STYPE = text,
  SFUNC = md5_agg_sfunc,
  INITCOND = ''
);
```

#### create pg testing table

We create the same table in the postgresql database as the one in oracle. 

```sql
DROP TABLE t_test cascade;
CREATE TABLE t_test (a int, b int, c text);
INSERT INTO t_test SELECT x, x + 10,'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' FROM generate_series(1, 50000) AS x;
```


### configuration

Create a configuration file "test.yaml" with:

```yaml
loglevel: INFO
source:
  name: src
  db:
    type: postgres
    host: localhost
    port: 5432
    username: postgres
    password: PGPASSWORD
    dbname: db1
    sslmode: require
    qry: select a,b,c from t_test order by a
target:
  name: tgt
  db:
    type: oracle
    init_oracle_client: "<path_to_oracle_client_libs>"
    host: localhost
    port: 1521
    username: system
    password: ORAPASSWORD
    service_name: XE
    qry: select a,b,c from t_test order by a
```

where
- "password" is the env name for storing oracle/pg passwords
- "qry" is the query executed to compare data. Be careful:
  -  the "order by" clause. All data must be in the same order to be compared.
  -  the number of fields must be identical
- "init_oracle_client" the path for oracle_instant_client path. It should contains file like: 
```log
BASIC_LICENSE            glogin.sql               libclntsh.dylib.19.1     libocci.dylib.12.1       libsqlplus.dylib         uidrvci
BASIC_README             libclntsh.dylib          libclntshcore.dylib.19.1 libocci.dylib.18.1       libsqlplusic.dylib       xstreams.jar
SQLPLUS_LICENSE          libclntsh.dylib.10.1     libnnz19.dylib           libocci.dylib.19.1       network
SQLPLUS_README           libclntsh.dylib.11.1     libocci.dylib            libociei.dylib           ojdbc8.jar
adrci                    libclntsh.dylib.12.1     libocci.dylib.10.1       libocijdbc19.dylib       sqlplus
genezi                   libclntsh.dylib.18.1     libocci.dylib.11.1       liboramysql19.dylib      ucp.ja
```

## Start the test with identical tables

```bash
cd scribedb
source venv/bin/activate
export PGPASSWORD=__your_pg_password__
export ORAPASSWORD=__your_ora_password__
python main.py -f scribedb/test.yaml
```

Scribedb should return you those logs:

```log
Nb Column are identical (3) on source and target
postgres Counting rows:50000
oracle Counting rows:50000
postgres Estimating round_trip, N° 1
postgres Estimating round_trip, N° 2
postgres Estimating round_trip, N° 3
postgres Estimating for 1 rows, N° 1
postgres Estimating for 1 rows, N° 2
postgres Estimating for 1 rows, N° 3
postgres Estimating for 100 rows, N° 1
postgres Estimating for 100 rows, N° 2
postgres Estimating for 100 rows, N° 3
postgres Estimating for 199 rows, N° 1
postgres Estimating for 199 rows, N° 2
postgres Estimating for 199 rows, N° 3
oracle Estimating round_trip, N° 1
oracle Estimating round_trip, N° 2
oracle Estimating round_trip, N° 3
oracle Estimating for 1 rows, N° 1
oracle Estimating for 1 rows, N° 2
oracle Estimating for 1 rows, N° 3
oracle Estimating for 100 rows, N° 1
oracle Estimating for 100 rows, N° 2
oracle Estimating for 100 rows, N° 3
oracle Estimating for 199 rows, N° 1
oracle Estimating for 199 rows, N° 2
oracle Estimating for 199 rows, N° 3
src can hash (41920) rows in 5000ms num_rows:50000
tgt can hash (20758) rows in 5000ms num_rows:50000
Total estimated time: [15]s
1/3 OK src hash:(36274DFE66BC9B452F0E8D511ACA012E) (in 135.717ms) 42%
1/3 OK tgt hash:(36274DFE66BC9B452F0E8D511ACA012E) (in 5586.614ms) 42%
2/3 OK src hash:(6C23916A00C5A185648F2A30F9ED2E5D) (in 176.897ms) 83%
2/3 OK tgt hash:(6C23916A00C5A185648F2A30F9ED2E5D) (in 7960.946ms) 83%
3/3 OK src hash:(4E9B04B7E45136188603DC0E575B584A) (in 78.357ms) 100%
3/3 OK tgt hash:(4E9B04B7E45136188603DC0E575B584A) (in 2378.877ms) 100%
Dataset are identicals
```

Success, the dataset have been detected identical.

## Non identical datasets

Let's change a line in the oracle database to check how scribedb will react: 

```sql
$ update t_test set c='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNO' where a=50;

1 row updated.

$ commit;

Commit complete.
```

Restarting scribedb: 

```bash
cd scribedb
source venv/bin/activate
export PGPASSWORD=__your_pg_password__
export ORAPASSWORD=__your_ora_password__
python main.py -f scribedb/test.yaml
```

Now, we can see an exception is raised: 

```log
Nb Column are identical (3) on source and target
postgres Counting rows:50000
oracle Counting rows:50000
postgres Estimating round_trip, N° 1
postgres Estimating round_trip, N° 2
postgres Estimating round_trip, N° 3
postgres Estimating for 1 rows, N° 1
postgres Estimating for 1 rows, N° 2
postgres Estimating for 1 rows, N° 3
postgres Estimating for 100 rows, N° 1
postgres Estimating for 100 rows, N° 2
postgres Estimating for 100 rows, N° 3
postgres Estimating for 199 rows, N° 1
postgres Estimating for 199 rows, N° 2
postgres Estimating for 199 rows, N° 3
oracle Estimating round_trip, N° 1
oracle Estimating round_trip, N° 2
oracle Estimating round_trip, N° 3
oracle Estimating for 1 rows, N° 1
oracle Estimating for 1 rows, N° 2
oracle Estimating for 1 rows, N° 3
oracle Estimating for 100 rows, N° 1
oracle Estimating for 100 rows, N° 2
oracle Estimating for 100 rows, N° 3
oracle Estimating for 199 rows, N° 1
oracle Estimating for 199 rows, N° 2
oracle Estimating for 199 rows, N° 3
src can hash (40855) rows in 5000ms num_rows:50000
tgt can hash (19872) rows in 5000ms num_rows:50000
Total estimated time: [15]s
1/3 NOK src hash:(4CD3A8D4EFAFB4D733649985C42994BC) (in 112.382ms) 40%
1/3 NOK tgt hash:(6E12FA362B03456CC7601ABEBD454F35) (in 4532.164ms) 40%
src:(50, 60, 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
tgt:(50, 60, 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNO')
2/3 OK src hash:(5D1FE7284E48A7F751672C7096F3FE98) (in 163.449ms) 79%
2/3 OK tgt hash:(5D1FE7284E48A7F751672C7096F3FE98) (in 6834.358ms) 79%
3/3 OK src hash:(0D9044601351A122E8A2D67401A6B83A) (in 72.73ms) 100%
3/3 OK tgt hash:(0D9044601351A122E8A2D67401A6B83A) (in 2371.928ms) 100%
Dataset are different
Exception
```

and the modified line had been retrieved correctly!