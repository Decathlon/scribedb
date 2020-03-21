# coding: utf-8
#!/usr/bin/python3

"""
Main objective

compare 2 databases (only the data for the moment). The server are named
"server1" and "server2"
it can be :
oracle to oracle comparaison
pgsql to oracle
oracle to pgsql
pgsql to pgsql

The process uses a pgsql repository database to store intermediate results. It
also can be restarted without reprocessing all the tasks (each steps are
stored in the repository database)

Returns: None
results are also stored in the repo database :
select * from tablediff  = > will store each table differences

select * from rowdiff  = > will store each rows differences (top 10)
"""


import psycopg2
import os
import logging
import socket
import sys
import postgres
import oracle

from threading import Thread


class ExecQry(Thread):
    """
    Class ExecQry used to execute queries on servers using threads : one
    thread for server1, one thread for server2. Both queries are executed and
    we wait until the end of the 2 executions to go forward.
    """

    def __init__(self, name,table, sql):
        Thread.__init__(self)
        self.sql = sql
        self.table = table
        self.result_exec = None
        self.excep = None
        self.name = name

    def run(self):
        """
        execute the query in a separate thread and wait for the result. it
        returns the dataset
        """
        rows = None
        if self.sql.startswith('select'):
            conn = self.table.connect()
            with conn.cursor() as curs:
                try:
                    curs.execute(self.sql)
                except conn.DatabaseError as exc:
                    error, = exc.args
                    logging.error(f"""error executing {self.sql}:
                    {error.code}""")
                    self.excep = exc
                    raise exc
                else:
                    rows = curs.fetchall()
        # logging.critical(f"""executed {self.sql}""")
        self.result_exec = rows

    def join(self, *args):
        """
        wait for the end of execute and return the dataset
        """
        Thread.join(self)
        if self.excep is not None:
            raise self.excep
        return self.result_exec


class Repo():
    """ The repo class represent the repository database.
    Its purpose is to store the temporary results, to prevent the reprocessing
    from the beginning.
    It also save the final result in rowdiff table
    """
    class Qry:
        def __init__(self, id=0, sqltext=None):
            self.id = id
            self.sqltext = sqltext

    class ResultMd5:
        def __init__(self, result, numrows):
            self.result = result
            self.numrows = numrows

    def __init__(self, cxRepo, schemaRepo, cxDb1, cxDb2, schemaDb1, schemaDb2, connect_timeout=20):
        self.cxRepo = cxRepo
        self.schemaRepo = schemaRepo
        self.cxDb1 = cxDb1
        self.cxDb2 = cxDb2
        self.schemaDb1 = schemaDb1
        self.schemaDb2 = schemaDb2
        self.connect_timeout = connect_timeout
        self.total_nbdiff = 0

    def connect(self, cxString):
        """
        return a database connection to postgres before executing query
        we use this method,  to be able to set particular session's parameters
        before execute
        Arguments:
          cxString {[string]} -- [connection string]
        Returns:
          [connection object]
        """
        conn = psycopg2.connect(cxString, connect_timeout=self.connect_timeout)
        s = socket.fromfd(conn.fileno(), socket.AF_INET, socket.SOCK_STREAM)
        # Enable sending of keep-alive messages
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Time the connection needs to remain idle before start sending
        # keepalive probes
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
        # Time between individual keepalive probes
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 1)
        # The maximum number of keepalive probes should send before dropping
        # the connection
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        return conn

    def split(self, table1, table2, step, high_limit, low_limit):
        """
        the process is divide in 4 steps :
        step 0 :  init table : generate a template query for the table. After
        this step,  the script stops. It give you the possibility to modify the
        template query for the following process.

        step 1 :  divide table in segment size limit_high.
                  Step 1 use md5 value computed on server1 and server2 to
                  compare dataset,  no data over the network.
                  It is a high cpu process consumer,  with low network traffic

        step 2 :  divide limit_high table in segment size limit_low
                  Step 1 use md5 value computed on server1 and server2 to
                  compare dataset, no data over the network.
                  It is a high cpu process consumer, with low network traffic

        step 3 : retreive each dataset from server and compare them with minus
        method
        It is a low cpu process consumer, but a high network traffic

        example :
        step 0 is splitted in step 1
        and then each step 1 is divide in step2 before processing

        Arguments:
          table {[string]} -- [table checked]
          step {[type]} -- [1 or 2]

        Returns:
          [dataset] -- split table from step 0 to step 1
        """

        def build_sql(start, stop):
            """ internal method to build qry from the step 0 template

            Arguments:
              start {int} -- lower bound value
              stop {int} -- high bound value

            Returns:
              [string] -- [insert 1 row in the repo.tablediff table
            """
            # logging.critical(f"""bld step {step} start = {start} stop ={stop}""")
            st = f"""select server1_qry,server2_qry from
            {self.schemaRepo}.tablediff where step = 0 and server1_qry is not
            null and server1_rows<>0 and server2_rows<>0 and lower(table_name)=
            lower('{table1.tableName}') order by id"""
            conn = self.connect(self.cxRepo)
            sql = None
            with conn.cursor() as curs:
                if st != "":
                    curs.execute(st)
                    qryrow = curs.fetchone()
                    if qryrow is not None:
                        qry1st = qryrow[0].replace("'", "''")
                        qry2st = qryrow[1].replace("'", "''")
                        qry1_select = table1.select.replace("'", "''")
                        qry2_select = table2.select.replace("'", "''")
                        sql = f"""INSERT INTO {self.schemaRepo}.tablediff
                        (cxstring1, schema1, cxstring2, schema2,table_name,
                        server1_select,server2_select,step, start, stop,
                        server1_qry,server2_qry,result)
                        SELECT '{table1.cxString}',
                        '{table1.schema}',
                        '{table2.cxString}',
                        '{table2.schema}',
                        '{table1.tableName}',
                        '{qry1_select}',
                        '{qry2_select}',
                        {step},{start},{stop},
                        '{qry1st}','{qry2st}','ready'
                        WHERE NOT EXISTS
                        (SELECT 1 FROM {self.schemaRepo}.tablediff WHERE
                        (table_name,
                        start,stop,step)=
                        ('{table1.tableName}' ,
                        {start},
                        {stop},
                        {step}))"""
            conn = self.connect(self.cxRepo)
#            logging.debug(f"""splitting qry:
#            {sql}""")
            with conn.cursor() as curs:
                try:
                    curs.execute(sql)
                except conn.DatabaseError as exc:
                    error, = exc.args
                    logging.error(
                        f"""error executing {sql} : {error}""")
            conn.commit()
            return None

        if step > 2:
            return 0
        # logging.critical(f"""split {table1.tableName} step {step}""")
        if step <= 1:
            maxrows = high_limit
        else:
            maxrows = low_limit

        """
        example : examine 1 row from tablediff

        id = 5747 (is the row id)
        step = 1 (step number 0 1 or 2)
        tablename = order_comment (tablename)
        server1_rows = 100 (number of rows processes during the md5
        calculation on the server1 the size comes from "high_limit":"100")
        server2_rows = 100 (number of rows processes during the md5
        calculation on the server2)
        start = 101 (where clause : numrows between :start and :stop)
        stop = 200 (where clause : numrows between :start and :stop)
        server1_hash = ce1be0ef6b605ea3674e607fd0e9da88 (the hash value of
        the partition on server1)
        server2_hash = 7e0af111be889d8cde86d1ed0cc36999 (the hash value of
        the partition on server2)
        server1_status = done (partition has been processes)
        server2_status = done (partition has been processes)
        result = nok (because hash1<>hash2)

        so the next step is :
        The size of the step 1 partition is 100 ("high_limit":"100").
        during step 2, this partition will be splitted in 10 partitions
        (size  = 10 "low_limit":"10")

        so the source here will create 10 lines in the tablediff from this
        partition with a status "ready"  :
        with its own start and stop values

        """
        sql = f"""select server1_rows,server2_rows,id,start from
        {self.schemaRepo}.tablediff where result <> 'ok' and step = {step-1}
        and lower(table_name) = lower('{table1.tableName}') order by id"""
        conn = self.connect(self.cxRepo)
        with conn.cursor() as curs:
            if sql != "":
                curs.execute(sql)
                rows = curs.fetchall()
                if rows is not None:
                    for row in rows:
                        if row[0] != 0 and row[1] != 0:
                            started = row[3]
                            if started == 0:
                                started = 1
                            maxnumrows = max(int(row[0]),int(row[1]))
                            nbStep = int(maxnumrows) // int(maxrows)
                            nbRest = int(maxnumrows) - nbStep * int(maxrows)
                            if nbStep > 0:
                                for i in range(nbStep):
                                    start = started + i * int(maxrows)
                                    stop = start + int(maxrows) - 1
                                    build_sql(start,stop)
                                if nbRest > 0:
                                    start = stop + 1
                                    stop = start + nbRest - 1
                                    build_sql(start,stop)
                            if nbStep == 0:
                                start = started
                                stop = start + nbRest - 1
                                build_sql(start,stop)

    def create(self):
        """
        create the postgresql repository schema needed
        this schema as tablediff and rowdiff table to save delta
        it is created in the cxString cxRepo.
        If not set then it use cxqString1
        """
        logging.debug(f"""create repo""")
        sql = f"""
              CREATE SCHEMA if not exists {self.schemaRepo};
              CREATE SEQUENCE if not exists {self.schemaRepo}.tablediff_id_seq;
              ALTER SEQUENCE if exists {self.schemaRepo}.tablediff_id_seq
              INCREMENT BY 1
              MINVALUE 1
              MAXVALUE 9223372036854775807
              START WITH 1
              NO CYCLE;
              CREATE TABLE if not exists {self.schemaRepo}.tablediff (
              id bigint,
              step integer,
              cxstring1 text,
              schema1 text,
              cxstring2 text,
              schema2 text,
              tips text,
              table_name text,
              server1_rows integer default 0,
              server1_select text,
              server2_rows integer default 0,
              server2_select text,
              start integer default 0,
              stop integer default 0,
              server1_qry text,
              server2_qry text,
              server1_hash text,
              server2_hash text,
              server1_status text default 'ready',
              server2_status text default 'ready',
              comments text,
              dti TIMESTAMP DEFAULT NOW(),
              dtu TIMESTAMP,
              result text default 'unknown'
              );
              CREATE OR REPLACE FUNCTION {self.schemaRepo}.fn_dtu() RETURNS TRIGGER
              LANGUAGE plpgsql
              AS $$
              BEGIN
                NEW.dtu := now();
              RETURN NEW;
              END;
              $$;
              DROP TRIGGER IF EXISTS dtu_update ON {self.schemaRepo}.tablediff
              CASCADE;
              CREATE TRIGGER dtu_update
                BEFORE UPDATE ON {self.schemaRepo}.tablediff
                  FOR EACH ROW
                    EXECUTE PROCEDURE {self.schemaRepo}.fn_dtu();
              CREATE SEQUENCE if not exists {self.schemaRepo}.rowdiff_id_seq;
              ALTER SEQUENCE if exists {self.schemaRepo}.rowdiff_id_seq
              INCREMENT BY 1
              MINVALUE 1
              MAXVALUE 9223372036854775807
              START WITH 1
              NO CYCLE;

              CREATE TABLE if not exists {self.schemaRepo}.rowdiff (
                id bigint,
                idtable bigint,
                table_name text,
                comments text,
                fields text,
                qry text,
                delta text
              );
              ALTER TABLE {self.schemaRepo}.tablediff ALTER id SET DEFAULT nextval('
              {self.schemaRepo}.tablediff_id_seq'::regclass);
              ALTER TABLE {self.schemaRepo}.rowdiff ALTER id SET DEFAULT nextval('
              {self.schemaRepo}.rowdiff_id_seq'::regclass);"""
        connRepo = self.connect(self.cxRepo)
        with connRepo.cursor() as curs:
            try:
                curs.execute(sql)
            except connRepo.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")

    def razcompare(self):
        """
        reset the schema repo to start a new compare
        """
        connRepo = self.connect(self.cxRepo)
        sql = "delete from {self.schemaRepo}.tablediff where step>0"
        with connRepo.cursor() as curs:
            curs.execute(sql)
        sql = f"""udpate {self.schemaRepo}.tablediff set server1_status =
        'ready',server2_status = 'ready',result = 'unkonwn'"""
        with connRepo.cursor() as curs:
            try:
                curs.execute(sql)
            except connRepo.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")
        connRepo.commit()

    def drop_schema(self):
        """
        drop the schema repo
        """
        connRepo = self.connect(self.cxRepo)
        sql = f"""drop schema if exists {self.schemaRepo} cascade"""
        with connRepo:
            with connRepo.cursor() as curs:
                curs.execute(sql)

    def get_queries(self, table, numserver):
        """
        return all the queries to be executed on server1 and server2

        example :

        a query must be executed if :
        status = ready
        table_name = table.tableName
        server1_hash or server2_hash is null (it has not been processed)

        Arguments:
          table {string} -- tha table being analyzed
          numserver {[type]} -- server1 or server2
        Returns:
          dataset of string (query)
        """
        returnvalue = None
        sql = f"""select id,server{numserver}_qry,start,stop from {self.schemaRepo}.tablediff
        where server{numserver}_hash is null
        and lower(schema{numserver}) = lower('{table.schema}')
        and lower(table_name) = lower('{table.tableName}')
        and server{numserver}_status = 'ready' and step>0
        order by id"""
        # logging.critical(f"""get_queries {table.tableName} numserver {numserver}
        # sql = {sql}""")
        conn = self.connect(self.cxRepo)
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                row = curs.fetchone()
                if row is not None:
                    id = row[0]
                    start = row[2]
                    stop = row[3]
                    limit = stop - start + 1
                    offset = start - 1
                    server_qry = f"{row[1]}"
                    formatted_q = server_qry.format(limit=limit,
                                                    offset=offset,
                                                    stop=stop,
                                                    start=start)
                    qry = Repo.Qry(id,formatted_q)
                    returnvalue = qry
        return returnvalue

    def set_qry(self, id, qry, numserver):
        """
        internal flag managment
        Arguments:
        id {int} --  id of tablediff row
        qry {string} -- qry executed to get md5
        numserver {int} -- numserver (1 or 2)
        """
        logging.debug(f"""set_qry for id {id} numserver {numserver}""")
        quotedqry = qry.replace("'","''")
        sql = f"""update {self.schemaRepo}.tablediff
        set server{numserver}_qry = '{quotedqry}'
        where id = {id}"""
        conn = self.connect(self.cxRepo)
        with conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(sql)
                except conn.DatabaseError as exc:
                    error, = exc.args
                    logging.error(f"""error executing {sql} : {error}""")

    def set_status(self, id, status, numserver):
        """
        internal flag managment
        Arguments:
          id {int} --  id of tablediff row
          status {string} -- value of status
            ready = ready to be processed
            running  = running
            done = processed
          numserver {int} -- numserver (1 or 2)
        """
        logging.debug(
            f"""__set_status {status} for id {id} numserver {numserver}""")
        sql = f"""update {self.schemaRepo}.tablediff
        set server{numserver}_status = '{status}'
        where id = {id}"""
        conn = self.connect(self.cxRepo)
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")
        conn.commit()

    def set_result(self, id, result):
        """
        internal result managment
        Arguments:
          id {int} -- id of tablediff row
          result {int} -- ok or nok
        """
        logging.debug(f"""__set_result {result} for id {id}""")
        sql = f"""update {self.schemaRepo}.tablediff
        set result = '{result}' where id = {id}"""
        conn = self.connect(self.cxRepo)
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")
        conn.commit()

    def set_comments(self, id, comments):
        """
        internal comments update managment
        Arguments:
          id {int} -- id of tablediff row
          comments {string} -- comments
        """
        logging.debug(f"""__set_comments {comments} for id {id}""")
        sql = f"""update {self.schemaRepo}.tablediff
        set comments = '{comments}' where id = {id}"""
        conn = self.connect(self.cxRepo)
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")
        conn.commit()

    def set_hash(self, id, ret, numserver):
        """
        internal result managment
        Arguments:
          id {[type]} --  id of tablediff row
          ret {[type]} -- ret : object struct {
              hash string (the hash calculated)
              rows int (the number of rows processed)
          }
          numserver {[type]} -- numserver (1 or 2)
        """

        if (ret == (None, None)) or (ret is None):
            hash = f"norows"
            rows = 0
        else:
            hash = ret.result
            rows = ret.numrows
        logging.info(
            f"""set_hash server{numserver} {hash} rows:{rows} for id {id}""")
        sql = f"""update {self.schemaRepo}.tablediff
        set server{numserver}_hash = '{hash}',
        server{numserver}_rows = {rows}
        where id = {id}"""
        conn = self.connect(self.cxRepo)
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")
        conn.commit()

    def insert_table_diff(self, table1, table2):
        """
        init step 0
        create the step 0, with its template for the following steps
        Arguments:
          table1 {object}
          table2 {object}
        """
        logging.debug(f"""step = 0 table_name = {table1.tableName}""")
        qry1 = table1.format_qry().replace("'","''")
        qry2 = table2.format_qry().replace("'","''")
   #     qry1 = qry1.replace("\"", "\"\"")
   #     qry2 = qry2.replace("\"", "\"\"")
        sql = f"""INSERT INTO {self.schemaRepo}.tablediff (
              cxstring1,
              schema1,
              cxstring2,
              schema2,
              table_name,
              server1_select,
              server2_select,
              server1_qry,
              server2_qry,
              server1_rows,
              server2_rows,
              result,
              step,
              comments)
              SELECT '{table1.cxString}','{table1.schema}','{table2.cxString}',
              '{table2.schema}','{table1.tableName}','{table1.select}',
              '{table2.select}','{qry1}','{qry2}',{table1.numrows},
              {table2.numrows},'init',0 ,'{table1.obs}' WHERE NOT EXISTS
              (SELECT 1 FROM {self.schemaRepo}.tablediff WHERE
              (cxstring1,schema1,lower(table_name),cxstring2,schema2,step)
              = ('{table1.cxString}', '{table1.schema}',
              lower('{table1.tableName}') ,'{table2.cxString}',
              '{table2.schema}',0))"""
        conn = self.connect(self.cxRepo)
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")
        conn.commit()

    def updateTableDiff(self, table1,table2):
        """
        init step 0
        update the step 0, with its numrows if where clause exist
        Arguments:
          table1 {object}
          table2 {object}
        """
        logging.debug(f"""updTblDiff table_name ={table1.tableName}""")
        qry1 = table1.format_qry().replace("'","''")
        qry2 = table2.format_qry().replace("'","''")
        server1_select = table1.select.replace("'","''")
        server2_select = table2.select.replace("'","''")
        sql = f"""update {self.schemaRepo}.tablediff set
              server1_rows = {table1.numrows},
              server2_rows = {table2.numrows},
              server1_select = '{server1_select}',
              server2_select = '{server2_select}',
              server1_qry = '{qry1}',
              server2_qry = '{qry2}'
              WHERE (lower(table_name),step)
              =(lower('{table1.tableName}'),0)"""
        conn = self.connect(self.cxRepo)
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")
        conn.commit()

    def reset_status(self):
        """

        reset_status, after restart.
        If a status is running after restart, then it was not stopped properly
         = > set it to null to process again the same row

        Returns:
          None
        """
        logging.debug(f"""reset_status""")
        conn = self.connect(cxRepo)
        sql = f"""update {self.schemaRepo}.tablediff set server1_status = null,
        server2_status = null where server1_status = 'running'"""
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")
        conn.commit()

    def update_table_result(self, table):
        """

        update global result for step 0.
        If all sub step have been processed (step>0), and the whole result is
        ok (IE, there is no rows with 'nok' result for this table), then the
        step 0 result is update from  'init' to 'ok'
        but if there is only 1 row with 'nok' then the global result (step 0)
        is set to 'nok'

        Returns:
          None
        """
        logging.debug(f"""update_table_result""")
        conn = self.connect(self.cxRepo)

        """ qry1 : table has been processed : everything is ok, there are no step > 0
        """
        sql = f"""update {self.schemaRepo}.tablediff set result = 'ok',
        server1_status = 'done',server2_status = 'done' , comments = '{table.obs}'
        where step = 0
        and lower(table_name) = lower('{table.tableName}')
        and server1_rows = server2_rows and server1_rows>0
        and not exists (select 1 from {self.schemaRepo}.tablediff
        where step>0 and server1_status = 'done'
        and server2_status = 'done' and result = 'nok'
        and lower(table_name) =lower('{table.tableName}'))"""
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error.code}""")

        """ qry2 : table has been processed : one step > 0 is not ok so the
        global result is nok
        """
        sql = f"""update {schemaRepo}.tablediff set result = 'nok',
        server1_status = 'done',server2_status = 'done', comments = '{table.obs}' 
        where step = 0
        and lower(table_name) = lower('{table.tableName}')
        and exists (select 1 from {schemaRepo}.tablediff where step>0
        and server1_status = 'done'
        and server2_status = 'done' and result = 'nok'
        and lower(table_name) =lower('{table.tableName}'))"""
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error.code}""")

        """ qry3 : table has been processed : server1_rows and server2_rows
        are <> so the global result is nok
        """
        sql = f"""update {schemaRepo}.tablediff set result = 'nok',
        server1_status = 'done',server2_status = 'done', comments = '{table.obs}'
        where step = 0 and lower(table_name) = lower('{table.tableName}')
        and server1_rows<>server2_rows and server1_status = 'ready'
        and server2_status = 'ready'"""
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")

        """ qry4 : table has been processed : server1_rows = server2_rows = 0
        (because fo filter) so the global result is ok
        """
        sql = f"""update {schemaRepo}.tablediff set result = 'ok',
        server1_status = 'done',server2_status = 'done' , comments = '{table.obs}' 
        where step = 0
        and lower(table_name) = lower('{table.tableName}')
        and server1_rows = server2_rows and server1_rows = 0
        and server1_status = 'ready' and server2_status = 'ready'"""
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")

        """ qry5 : table has been processed : server1_rows = server2_rows = -1
        (because of norows or no pk) so the global result is nok
        """
        sql = f"""update {schemaRepo}.tablediff set result = 'nok',
        server1_status = 'done',server2_status = 'done', comments = '{table.obs}'
        where step = 0
        and lower(table_name) = lower('{table.tableName}')
        and server1_rows = server2_rows and server1_rows = -1
        and server1_status = 'ready' and server2_status = 'ready'"""
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")

        conn.commit()

    def get_tables(self):
        """

        get list table to compute from repo

        Returns:
          dataset of tablename
        """
        logging.debug(f"""get_tables""")
        conn = self.connect(cxRepo)
        sql = f"""select table_name,server1_select,server2_select,schema1,
        schema2,tips from {self.schemaRepo}.tablediff
        where step = 0 and result = 'init' order by id"""
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")
            rows = curs.fetchall()
        return rows

    def exists(self, cxRepo, schemaRepo, schema, tablename):
        """

        check if table already initialized

        Returns:
          true / false
        """
        logging.debug(f"""check if {tablename} exists step 0""")
        conn = self.connect(cxRepo)
        sql = f"""select * from {schemaRepo}.tablediff where lower(table_name)
        = lower('{tablename}') and lower(schema1) = lower('{schema}')"""
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(f"""error executing {sql} : {error}""")
            row = curs.fetchone()

        if row is None:
            return 0
        else:
            return 1

    def ifAlreadyDone(self, cxRepo, schemaRepo, schema, tablename):
        """

        check if table already analyzed

        Returns:
          true / false
        """
        logging.debug(f"""check if {schema}.{tablename} has been analyzed""")
        conn = self.connect(cxRepo)
        sql = f"""select table_name from {schemaRepo}.tablediff where lower
        (table_name) = lower('{tablename}') and schema1 = '{schema}' and
        server1_status = 'ready' and server1_status  = 'ready' and result in
        ('ready', 'init')"""
        with conn.cursor() as curs:
            curs.execute(sql)
            row = curs.fetchone()
        if row is None:
            return 1
        else:
            return 0

    def compute_diffrowset(self, table1, table2):
        """
        use after step 2, it is the last step
        the number of rows to compute is lower than the "low_limit" value. So
        we will not one md5 value for all this rows.
        We will retreive the dataset of the 2 servers, and then compare them
        together.
        all differences are written in rowdiff table.

        Arguments:
          table1 {object}
          table2 {object}

        Returns:
          None
        """

        logging.info(f"""compute_diffrowset {table1.tableName}""")

        def build_where(table, result_row):
            i = 0
            where_clause = ""
            rowslist = table.get_pk()
            rows = rowslist.split(',')
            pk_idxlist = table.get_pk_idx()
            pk_idx = pk_idxlist.split(',')
            for result_col in rows:
                idx = int(pk_idx[i])
                rst_row = result_row[idx - 1]
                i = i + 1
                # remove
                # if rst_row == "3800497307669":
                #    logging.info(f"""cdebug""")
                if rst_row is not None:
                    columnname = result_col
                    value_utf8 = ""
                    field_type = table.get_field_datatype(
                        table.viewName,columnname)
                    if ("char".upper() in field_type.upper()) or (
                            "text".upper() in field_type.upper()):
                        if type(rst_row) is int:
                            value_from_db = str(rst_row)
                        else:
                            value_from_db = rst_row
                        try:
                            value_utf8 = value_from_db.encode('utf-8')
                        except Exception as e:
                            error, = e.args
                            logging.error(
                                f"""error executing encode for {value_from_db} : {error}""")

                        quote = "'"
                        where_clause = where_clause + ' AND ' + columnname + \
                            ' = ' + quote + \
                            value_utf8.decode(
                                'utf-8').replace("'","''") + quote
                    elif (("timestamp".upper() in field_type.upper()) or ("date".upper() in field_type.upper())):
                        st1 = "to_date('"
                        st2 = "','YYYY-MM-DD HH24:MI:SS')"
                        where_clause = where_clause + ' AND ' + columnname + \
                            ' = ' + st1 + str(rst_row) + st2
#                    elif ("date".upper() in field_type.upper()):
#                        st1 = "to_date('"
#                        st2 = "','YYYY-MM-DD HH24:MI:SS')"
#                        if ("." in str(rst_row)):
#                            st1 = "to_timestamp('"
#                            st2 = "','YYYY-MM-DD HH24:MI:SS.FF')"
#                        where_clause = where_clause + ' AND ' + columnname + \
#                            ' = ' + st1 + str(rst_row) + st2
                    else:
                        where_clause = where_clause + ' AND ' + \
                            columnname + ' = ' + str(rst_row)
            return where_clause

        def format_result(result):
            """[summary]
            will seek all differences between 2 datasets, and then will query
            server1 and server2, to check if rows does not exists or is really
            different.

            When dataset are <> it does not means that the missing row is not
            in an other partition.
            for example, we used to have this problem : "when there is a text
            field in then primary key, the order by clause with uppercase and
            lowercase in oracle and pgsql are different. Lowercase is first in
            pgsql, but in oracle uppercase comes first."
            This problem was solved by Pierre Caron using the order by collate
            "c" on character fields.

            Oracle :
                select * from foo order by ch;
                CH
                ----------
                1
                A
                a

              PostgreSQL :
                select * from foo order by ch;
                ch
                ----
                1
                a
                A

                select * from foo order by ch collate "C";
                ch
                ----
                1
                A
                a

            We keep this control, even if it is not needed, we should face some
            new differences between the 2 db engines.

            an other use case :

                  oracle                pgsql
                  table1 (c1,c2,c3)     table1(c1,c2,c3)
                  c1    c2    c3        c1    c2    c3
            line1  1     1     1         1     1     1
            line2  2     2     2         3     3     3
            line3  3     3     3         4     4     4
            line4  4     4     4         6     6     6

            partition1 = line1 and line2, we compare oracle and pgsql, we can
            see <> (333 is missing in oracle, but it exists in the 2nd
            partition
            (line3). So format_result see that 333 is missing in partition1 of
            oracle, but it must check that it does not exists in an other
            partition (partition2 line 3). As it exists then nb_error = 0


            Returns:
              [int] -- number of errors
            """
            nbdiff = 0
            errloc = 0
            i = 0
            total_diff = len(result)
            for result_row in result:
                i = i + 1
                logging.info(f"""search row {i}/{len(result)}""")
                if nbdiff >= int(maxdiff):
                    logging.warning(
                        f"""line {id}:reach max diff {maxdiff} for {table1.schema}.{table1.tableName} total diff:{total_diff}""")
                    errloc = nbdiff
                    self.total_nbdiff = nbdiff
                    break
                list_fields = ''
                fields1 = table1.concatened_fields
                fields2 = table2.concatened_fields
                qry1_fields = f"""select {fields1} from
                {table1.schema}.{table1.viewName}
                where
                1 = 1 """
                qry2_fields = f"""select {fields2} from
                {table2.schema}.{table2.viewName}
                where
                1 = 1 """

                qry1 = qry1_fields + build_where(table1,result_row)
                qry2 = qry2_fields + build_where(table2,result_row)
              #  list_fields = '|'
              #  list_fields = list_fields.join(result_row)

                for result_col in result_row:
                    test = "{}"
                    list_fields = list_fields + '|' + test.format(result_col)
              #      if type(result_col) is str:
              #            list_fields = ''.join(list_fields,'|',result_col)
              #        else:
              #            list_fields = ''.join(list_fields,'|',result_col)

                    # .encode
                    # ('utf-8').strip()
                    # list_fields = list_fields + '|' + result_col.encode('utf-8')

                list_fields = list_fields.lstrip('|')
                quotedsql = qry1.replace("'","''")

                qry_thread_1 = ExecQry(
                    table1.getengine() + '_dtsDiff',table1,qry1)
                qry_thread_2 = ExecQry(
                    table2.getengine() + '_dtsDiff',table2,qry2)

                """
                start the threads on server1 and server2
                """
                qry_thread_1.start()
                qry_thread_2.start()

                """
                wait for the 2 thread to terminate
                """
                try:
                    """
                wait for the qry being executed
                """
                    row1detail = qry_thread_1.join()
                except Exception as exc:
                    error, = exc.args
                    logging.error(f"""error executing thread:
                    {error.code}""")
                try:
                    """
                    wait for the qry being executed
                    """
                    row2detail = qry_thread_2.join()
                except Exception as exc:
                    error, = exc.args
                    logging.error(f"""error executing thread:
                    {error.code}""")

                if (row1detail is not None) and (len(row1detail) != 0):
                    nbrows1 = 1
                    fieldst1 = row1detail[0][0]
                    fieldst1 = fieldst1.replace("\x00","")
                    fieldst1 = fieldst1.replace("\r\n","\n")
                    fieldst1 = fieldst1.replace(" \n","\n")
                    fieldst1 = fieldst1.replace("\t","")
                    if fieldst1 != row1detail[0][0]:
                        self.set_comments(id,'x00 or other found')
                else:
                    nbrows1 = 0

                if (row2detail is not None) and len(row2detail) != 0:
                    nbrows2 = 1
                    fieldst2 = row2detail[0][0]
                    fieldst2 = fieldst2.replace("\x00","")
                    fieldst2 = fieldst2.replace("\r\n","\n")
                    fieldst2 = fieldst2.replace("\t","")
                    fieldst2 = fieldst2.replace(" \n","\n")
                    if fieldst2 != row2detail[0][0]:
                        self.set_comments(id,'x00 or other found')
                else:
                    nbrows2 = 0

                desc = "ok"
                if nbrows1 == 1 and nbrows2 == 1:
                    if fieldst1 != fieldst2:
                        desc = f"""( <> in server1 {table1.getengine()} and server2 {table2.getengine()})
                    server1 {fieldst1}
                    server2 {fieldst2}"""
                    # logging.info(f"""delta in {table1.tableName}:\n {desc}
                    # """)
                elif nbrows1 == 1 and nbrows2 == 0:
                    desc = f"""(+ in server1 {table1.getengine()}) {fieldst1} ; (- in server2 {table2.getengine()}) """
                elif nbrows1 == 0 and nbrows2 == 1:
                    desc = f"""(- in server1 {table1.getengine()}) ; (+ in server2 {table2.getengine()}) {fieldst2}"""

                quoteddesc = desc.replace("'","''")
                quotedlist_fields = list_fields.replace("'","''")
                quotedtableName = table1.tableName.replace("'","''")
                sql = f"""insert into {schemaRepo}.rowdiff (idtable,table_name,
                comments,fields,qry) select '{id}','{quotedtableName}','
                {quoteddesc}','{quotedlist_fields}','{quotedsql}'
                where not exists
                (select 1 from {schemaRepo}.rowdiff where (idtable,lower
                (table_name),
                comments,qry) =
                ({id},lower('{quotedtableName}'),'{quoteddesc}','{quotedsql}'))"""

                if desc != 'ok':
                    errloc = errloc + 1
                    nbdiff = nbdiff + 1
                    logging.info(
                        f"""diffrowset nok {table1.tableName} for id = {id}""")
                    logging.error(f"""{desc}""")
                    conn = self.connect(cxRepo)
                    with conn.cursor() as curs:
                        try:
                            curs.execute(sql)
                        except Exception as exc:
                            error, = exc.args
                            logging.error(f"""error executing {sql}:
                            {error.code}""")
                    conn.commit()
            return errloc

        """
        list all rows with step = 2 (or step = 1 without step = 2) and result
        = 'nok' and status = 'done'
        """
        quotedtableName = table1.tableName.replace("'","''")
        sql = f"""select id,start,stop from {self.schemaRepo}.tablediff where (step = 2
        or (step = 1 and not exists (select 1 from {self.schemaRepo}.tablediff
        where
        step = 2
        and lower(table_name) = lower('{quotedtableName}')
        and lower(schema1) = lower('{table1.schema}'))))
        and result = 'nok'
        and server1_status = 'done'
        and server2_status = 'done'
        and lower(table_name) = lower('{quotedtableName}')
        and lower(schema1) = lower('{table1.schema}')
        order by id"""

        """
        for each of them, execute qry on separate thread to retreive datasets
        """
        nberr = 1
        logging.debug(f"""qry compute_diffrowset : {sql}""")
        conn = self.connect(self.cxRepo)
        with conn.cursor() as curs:
            curs.execute(sql)
            rows = curs.fetchall()
            if rows is not None:
                for row in rows:
                    if self.total_nbdiff != 0:
                        break
                    id = row[0]
                    start = row[1]
                    stop = row[2]
                    logging.debug(
                        f"""search diff in {table1.tableName} range : {start} -> {stop} retreiving dataset from server1 & 2...""")
                    qry1 = table1.format_qry_last(start,stop)
                    qry2 = table2.format_qry_last(start,stop)

                    qry_thread_1 = ExecQry(
                        table1.getengine() + '_dtsFetch',table1,qry1)
                    qry_thread_2 = ExecQry(
                        table2.getengine() + '_dtsFetch',table2,qry2)

                    self.set_status(id,'running',1)
                    self.set_status(id,'running',2)

                    qry_thread_1.start()
                    qry_thread_2.start()

                    try:
                        """
                        wait for the qry being executed
                        """
                        rows1 = qry_thread_1.join()
                    except Exception as exc:
                        error, = exc.args
                        logging.error(f"""error executing thread:
                        {error.code}""")
                    try:
                        """
                        wait for the qry being executed
                        """
                        rows2 = qry_thread_2.join()
                    except Exception as exc:
                        error, = exc.args
                        logging.error(f"""error executing thread:
                        {error.code}""")

                    """"
                    2 datasets are ready to be compared
                    """
                    if rows1 is not None:
                        rowsets1 = set(rows1)
                        if rows2 is not None:
                            rowsets2 = set(rows2)
                            result_a_b = rowsets1 - rowsets2
                            result_b_a = rowsets2 - rowsets1
                            nberr = 0
                            nberr2 = 0
                            nberr1 = 0
                            if len(result_a_b) > 0:
                                """
                                there are some <> inside
                                """
                                logging.info(
                                    f"""find {len(result_a_b)} in {table1.getengine()}/{len(rowsets1)}""")
                                nberr1 = format_result(result_a_b)
                            if nberr1 < int(maxdiff) and len(result_b_a) > 0:
                                """
                                there are some <> inside
                                """
                                logging.info(
                                    f"""find {len(result_b_a)} in {table2.getengine()}/{len(rowsets2)}""")
                                nberr2 = format_result(result_b_a)
                            nberr = nberr1 + nberr2
                            if nberr == 0:
                                self.set_result(id,'ok')
                            else:
                                self.set_result(id,'nok')
                    else:
                        self.set_result(id,'nok')

                    self.set_status(id,'done',1)
                    self.set_status(id,'done',2)
        return nberr

    def compute_md5(self, table1, table2):
        """

        use for step 1.
        the number of rows to compute is higher than the "high_limit" value.
        the table is divide in several partition.
        For each row of each partition, a row md5 is generated. This md5 is
        then concatened with the md5 of the previous row

        this method execute queries on server1 and server2 in 2 separate
        thread. So the 2 queries are started and we wait for them.

        So for each partition, we have only 1 md5 value

        This md5 value is updated in tablediff

        Arguments:
          table {object}
        """
        logging.info(
            f"""compute_md5 {table1.tableName} in 2 threads ora_Hash & pg_Hash""")

        """
        get the queries to be executed
        """
        qry1 = self.get_queries(table1,1)
        qry2 = self.get_queries(table2,2)

        err = 0
        maxnumrows1 = 0
        result_md5 = self.ResultMd5('',0)

        if qry1 is None:
            err = 1

        """
        there is qry to execute
        """
        while (qry1 is not None):

            if table1.numrows == table2.numrows:

                """
                create the 2 threads objects to execute qry
                """
                qry_thread_1 = ExecQry(
                    table1.getengine() + '_Hash',table1,qry1.sqltext)
                qry_thread_2 = ExecQry(
                    table2.getengine() + '_Hash',table2,qry2.sqltext)

    #            logging.debug("thread1 = " + qry_thread_1.name + "qry = " + qry1.
    #                          sqltext)
    #            logging.debug("thread2 = " + qry_thread_2.name + "qry = " + qry2.
    #                          sqltext)

                self.set_status(qry1.id,'running',1)
                self.set_status(qry2.id,'running',2)

                self.set_qry(qry1.id,qry1.sqltext,1)
                self.set_qry(qry2.id,qry2.sqltext,2)

                """
                start the threads on server1 and server2
                """
                try:
                    qry_thread_1.start()
                except Exception:
                    logging.error("thread error")
                    break
                try:
                    qry_thread_2.start()
                except Exception:
                    logging.error("thread error")
                    break

                """
                wait for the 2 thread to terminate
                """
                r1 = qry_thread_1.join()
                r2 = qry_thread_2.join()
                ret1 = self.ResultMd5('',0)
                ret2 = self.ResultMd5('',0)
                ret1.result = r1[0][0]
                ret1.numrows = r1[0][1]
                ret2.result = r2[0][0]
                ret2.numrows = r2[0][1]
            else:
                ret1 = self.ResultMd5(
                    table1.getengine() + ' nbrows<>' + str(table1.numrows),table1.numrows)
                ret2 = self.ResultMd5(
                    table2.getengine() + ' nbrows<>' + str(table2.numrows),table2.numrows)
                err = 1

            """
            Fill some flag values to represent the status and result
            """
            vhash1 = None
            hash1 = ''
            vhash2 = None
            hash2 = ''
            numrows1 = 0
            result = 'nok'

            if ret1 is not None:
                vhash1 = ret1
                hash1 = ret1.result
                numrows1 = ret1.numrows

            if ret2 is not None:
                vhash2 = ret2
                hash2 = ret2.result

            if numrows1 > maxnumrows1:
                maxnumrows1 = numrows1

            self.set_hash(qry1.id,vhash1,1)
            self.set_hash(qry2.id,vhash2,2)

            """
            set the result of this partition
            """

            if ((hash1 != '') and (hash2 != '')):
                if (hash1 == hash2) and (hash1 != 'norows'):
                    result = 'ok'
                else:
                    err = err + 1

            self.set_result(qry1.id,result)

            """
            tell that this partition has been processed
            """
            self.set_status(qry1.id,'done',1)
            self.set_status(qry2.id,'done',2)

            """
            get the next query for step 1
            """
            qry1 = self.get_queries(table1,1)
            qry2 = self.get_queries(table2,2)

        if err > 0:
            result = 'nok'
            result_md5 = self.ResultMd5(result,maxnumrows1)

        return result_md5


def init(schema1, schema2):
    """
    entrypoint:

    create the repo schema
    create the table1 and table2 objects (for server1 and server2)

    The first execution will create the first step (0), with the query
    template. and then it stops. You can modify the query template.

    The 2nd execution will process all tables created in the 1st execution.


    split the step 0 in n partitions, those partitions are in the step1. its
    size = "high_limit" value.

    compute the hash value of each of them

    for each row of step1, if server1_hash <> server2_hash then, table is we
    go to step2 :
      the partition is splitted in n subpartitions ( its size = "low_limit"
      value)
      and then md5 is not calculated, but dataset are retreive from each
      database server.

    Arguments are in env variables

    Arguments:
      high_limit {int} -- the upper bound limit for step 1 example 300000]
      low_limit {int} -- the lower bound limit for step 2 example 60000]
      cxstring2 {string} -- connection string to server2 example user/
      xxxxx@localhost:1521/XE
      cxstring1 {string} -- connection string to server2 example postgresql://
      user:xxxxx@localhost:5432/orders_as1_iso
      cxRepo {string} -- connection string to repo (pgsql only) example
      :"postgresql://useroxxxxxo@localhost:5432/orders_as1_iso",
      schema1 {string} -- schema on server 1 example orders_as1_iso
      schema2 {string} -- schema on server 2 orders_as1_iso
      schema {string} -- schema repo name example dbdiffdml
      log {string} -- log level info  critical,error,warning,info,debug
      (default = warning)
      include {string} -- to include table to check. it is the where clause
      default = true (all tables), it can be
                          table_name in ('ORDER_COMMENT') (for oracle)
                          table_name in ('order_comment') (for pgsql)
    """

    """
    create the dbdiffdml schema and its objects to save results
    """

    repo = Repo(cxRepo,
                schemaRepo.lower(),
                cxstring1,cxstring2,
                schema1.lower(),
                schema2.lower()
                )
    repo.create()

    """
    if a previous run was interrupted, then we must reset the status, to
    reprocess the row
    """
    repo.reset_status()
    scribedb_return = 0

    if cxstring1.startswith('postgresql'):
        table1 = postgres.Table(cxstring1,schema1,schemaRepo.lower())
        logging.info("server1 is a postgresql db")
    else:
        table1 = oracle.Table(cxstring1,schema1,schemaRepo.lower())
        logging.info("server1 is an oracle db")

    if cxstring2.startswith('postgresql'):
        table2 = postgres.Table(cxstring2,schema2,schemaRepo.lower())
        logging.info("server2 is a postgresql db")
    else:
        table2 = oracle.Table(cxstring2,schema2,schemaRepo.lower())
        logging.info("server2 is an oracle db")
    """
    get list of tables from server1 and add them to tablediff with step = 0
    """
    listTables = table1.get_tablelist(qry_include_table)
    phase = 'compute'
    for table in listTables:
        tablename = table[0]
        if repo.exists(cxRepo,schemaRepo,schema1,tablename) == 0:
            logging.info(f"""initializing table {tablename}""")
            table1.create(tablename,schema1,None,False)
            table2.create(tablename,schema2,None,False)
            repo.insert_table_diff(table1,table2)
            phase = 'init'
            msg = f"""all tables have been initiliazed you can see them with
            \n select * from {schemaRepo}.tablediff where step = 0\n
              you can modify the template. The 2nd execution will used the
              step = 0 query to process them.\n
              """
        else:
            logging.info(f"""{tablename} initialized""")

    if phase == 'compute':
        listTables = repo.get_tables()
        if len(listTables) == 0:
            logging.info(f"""no table to compare on {schemaRepo}""")
        else:
            for table in listTables:
                tableName = table[0]
                server1_select = table[1]
                server2_select = table[2]
                schema1 = table[3]
                schema2 = table[4]
                tips = table[5]
                if repo.ifAlreadyDone(cxRepo,
                                      schemaRepo,
                                      schema1,
                                      tableName) == 0:
                    step = 0
                    logging.info(
                        f"""processing table {tableName} (counting rows on 2 dbs, creating objects...)""")

                    table1.create(tableName,schema1,server1_select)
                    if table1.pk == '':
                        logging.info(
                            f"""{table1.getengine()} {tableName} has no pk""")
                        repo.update_table_result(table1)
                        continue
                    if table1.numrows == 0:
                        logging.info(
                            f"""{table1.getengine()} {tableName} is empty""")
                        repo.update_table_result(table1)
                        continue
                    logging.info(
                        f"""{table1.getengine()} {tableName} numrows: {table1.numrows} / nbFields: {table1.getNbFields()}""")

                    table2.create(tableName,schema2,server2_select)
                    if table2.get_pk() is None:
                        logging.info(
                            f"""{table1.getengine()} {tableName} has no pk""")
                        repo.update_table_result(table2)
                        continue
                    if table2.numrows == 0:
                        logging.info(
                            f"""{table2.getengine()} {tableName} is empty""")
                        repo.update_table_result(table2)
                        continue
                    logging.info(
                        f"""{table2.getengine()} {tableName} numrows: {table2.numrows} / nbFields: {table2.getNbFields()}""")

                    if table2.getNbFields() != table1.getNbFields():
                        logging.info(
                            f"""number of fields are differents, can not compare such datasets""")
                        repo.update_table_result(table1)
                        continue

                    """ update server1_rows and server2_rows if there is where
                    clause"""
                    logging.info(
                        f"""{table1.getengine()} type: {table1.getDataTypeFields()}""")
                    logging.info(
                        f"""{table2.getengine()} type: {table2.getDataTypeFields()}""")
                    repo.updateTableDiff(table1,table2)

                    while step < 2:
                        step = step + 1
                        logging.info(
                            f"""split table {tableName} step {step}""")
                        repo.split(table1,table2,step,high_limit,low_limit)
                        check_md5 = repo.compute_md5(table1,table2)
                        if check_md5.result == '':
                            break
                        if check_md5.result == 'nok':
                            if (step == 1 and int(check_md5.numrows) < int(low_limit)
                                ) or (step == 2
                                      ):
                                if tips is None:
                                    ret = repo.compute_diffrowset(
                                        table1,table2)
                                else:
                                    ret = 1
                                scribedb_return = scribedb_return + ret
                                if ret > 0:
                                    logging.warning(
                                        f"Error on table {tableName}")
                                break
                else:
                    logging.info(f"{tableName} already processed")
                repo.update_table_result(table1)
                logging.info(f"{tableName} processed")
                table1.drop_view()
                table2.drop_view()
    else:
        logging.info(msg)
    return scribedb_return


if __name__ == '__main__':
    argv = sys.argv[1:]
    help_msg = """params: --step = init|compare --cxstring1 = <postgresql://
    <pg_user>:<pg_pwd>@<pg_host>:<pg_port>/<pg_db> --cxstring2 = <ora_user>/
    <ora_pwd>@<ora_server>:<ora_port>/<ora_servicename> --schema1 = <pgsql
    schema> --schema2 = <ora schema> --include = <qry to include table ex
    'true' or 'table_name in (''mytable1'',''mytable2'')'> or set env var
    cxstring1,cxstring2,schema1,schema2"""
    os.environ["PYTHONIOENCODING"] = "utf-8"
    schemaRepo = os.environ["schemarepo"]
    low_limit = os.environ.get("low_limit", 50001)
    high_limit = os.environ.get("high_limit", 100000)
    maxdiff = os.environ.get("maxdiff", 50)
    log = os.environ.get("log", 'warning')
    cxstring1 = os.environ.get("cxstring1", None)
    cxstring2 = os.environ.get("cxstring2", None)
    cxRepo = os.environ.get("cxrepo", None)
    schema1 = os.environ.get("schema1", None)
    schema2 = os.environ.get("schema2", None)
    qry_include_table = os.environ.get("qry_include_table", "true")
    step = os.environ.get("step","init+compute")
    if cxRepo is None:
        cxRepo = os.environ.get("cxRepo", None)
    if cxRepo is None:
        cxRepo = cxstring1
    numeric_level = getattr(logging, log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log)
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    gstep = 0
    # divise les tables par 10 pour recherche de diff
    if (cxstring1 is None) or (
       cxstring2 is None) or (schema1 is None) or (schema2 is None):
        print(help_msg)
        sys.exit(2)

    if step == "init+compute":
        scribedb_return = init(schema1,schema2)
        scribedb_return = init(schema1,schema2)
    else:
        scribedb_return = init(schema1,schema2)

    logging.debug(f"exit code:{scribedb_return}")
    scribedb_return = 0
    sys.exit(scribedb_return)
