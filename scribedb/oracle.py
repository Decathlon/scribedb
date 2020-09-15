import logging
import cx_Oracle
from rdbms import Table as TableRdbms


class Table(TableRdbms):

    dbEngine = "oracle"

    """
    class table to represent a table in the repo structure
    example :
    all example are based on the table HR.EMPLOYEES
    """

    def __init__(self, cxString, schema, schemarepo):
        TableRdbms.__init__(self,cxString,schema, schemarepo)
        self.conn = self.connect()
        self.create_ora_objects(cxString)

    def connect(self):
        """
        return a database connection to Oracle before executing query
        if the database is oracle, we change some session parameters
        (NLS_TIMESTAMP_FORMAT), to be sure that date between pgsql and oracle
        ll be display in the same format.

        Returns:
          [connection object]
        """
        sqlSetSession = f"""alter session set NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"""
        conn = cx_Oracle.connect(self.cxString)
        curs = conn.cursor()
        try:
            curs.execute(sqlSetSession)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            logging.error(
                f"""{self.dbEngine}:sqlSetSession : {error.message}""")

        sqlSetSession = f"""alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"""
        conn = cx_Oracle.connect(
            self.cxString,encoding="UTF-8",nencoding="UTF-8")
        curs = conn.cursor()
        try:
            curs.execute(sqlSetSession)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            logging.error(
                f"""{self.dbEngine}:sqlSetSession : {error.message}""")
        return conn

    def create_ora_objects(self, cxString):
        """
        create oracle object needed to calculate md5 values for all fields

        before execute be sure (as sysdba): grant execute on dbms_crypto to
        oracle_user, grant resource to user_oracle

        Arguments:
          cxString {string} -- [the connetion string to oracle]
        """
        logging.debug(f"""{self.dbEngine}:create oracle objects if needed""")
        sql = """CREATE or REPLACE FUNCTION HASH_MD5 (
        psINPUT IN clob
        ) RETURN VARCHAR2 AS
        rHash RAW (16);
        BEGIN
        rHash := DBMS_CRYPTO.HASH (psINPUT, DBMS_CRYPTO.HASH_MD5);
        RETURN (LOWER(RAWTOHEX (rHash)));
        END HASH_MD5;
        """
        logging.info(
            f"""{self.dbEngine}:before execute be sure (as sysdba): grant execute on dbms_crypto to oracle_user, grant resource to user_oracle""")
        conn = self.connect()
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(
                    f"""{self.dbEngine}:error executing {sql} : {error.code}""")

    def get_tablelist(self, qry_include_table):
        """

        return the table list to compute from database catalog

        Arguments:
          qry_include_table {[type]} -- where clause to include ex : and
          {qry_include_table} default = true

        Returns:
          [dataset] -- [list tables]
        """

        logging.debug(f"""{self.dbEngine}:get_tablelist""")
        sql = f"""select lower(table_name),num_rows from all_all_tables where
        upper(owner) = upper('{self.schema}') and {qry_include_table}
              order by num_rows,table_name"""
        conn = self.connect()
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(
                    f"""{self.dbEngine}:error executing {sql} : {error.code}""")
            rows = curs.fetchall()
        return rows

    def get_field_datatype(self, object_name, column):
        """
        used during dynamic query building, to eventually add quote or not.

        Arguments:
          column {string} -- the column name you need the type

        example
        input : LAST_NAME
        output : VARCHAR2(25 BYTE)

        Returns:
          string  datatype
        """
        s_col = self.sanitize_name(column)
        sql = f"""SELECT data_type FROM
        all_tab_columns
        where
        upper(owner) = upper('{self.schema}')
        and upper(table_name) = upper('{object_name}')
        and upper(column_name) = upper('{s_col}')"""
        conn = self.connect()
        with conn.cursor() as curs:
            try:
                curs.execute(sql)
            except conn.DatabaseError as exc:
                error, = exc.args
                logging.error(
                    f"""{self.dbEngine}:error executing {sql} : {error.code}""")
            row = curs.fetchone()
            if row is None:
                logging.error(f"""{self.dbEngine}:table:#{object_name}# column #{column}# not
                found sql:#{sql}#""")
        return row[0]

    def drop_view(self):
        """
        see in herited classe for details

        drop temporary view to be able to get the datatype for cast
        drop the view if exists
        """
        logging.debug(
            f"""{self.dbEngine}:drop view for {self.schema}.{self.viewName}""")
        sql = f"""drop view {self.schema}.{self.viewName}"""
        conn = self.connect()
        with conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(sql)
                except Exception as e:
                    error, = e.args
                    if error.code != 942:
                        logging.debug(
                            f"""{self.dbEngine}:error drop_views executing {sql} {error}""")
        sql = f"""drop view {self.schema}.{self.viewName}_c"""
        conn = self.connect()
        with conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(sql)
                except Exception as e:
                    error, = e.args
                    if error.code != 942:
                        logging.debug(
                            f"""{self.dbEngine}:error drop_views executing {sql} {error}""")

    def cast_field(self, field, datatype):
        # if "TIMESTAMP" in datatype:
        #    st = f"""to_char({field},'YYYY-MM-DD HH24:MI:SS.FF')||'|'||"""
        if ("DATE" in datatype) or ("TIMESTAMP" in datatype):
            st = f"""to_char({field},'YYYY-MM-DD HH24:MI:SS')||'|'||"""
        elif "DOUBLE PRECISION" in datatype or "NUMBER" in datatype:
            st = f"""rtrim(to_char({field}, 'FM999999999999990.999999'),
            '.')||'|'||"""
        elif "CHAR" in datatype:
            """
            oracle : for example CHAR(10) if filled with space until 10, not
            in pgsql. So we have to remove uneeded space at the end,
            to compare both.
            """
            st = f"""rtrim({field})||'|'||"""
        else:
            st = f"""{field}||'|'||"""
        return st

    def set_fields(self, object_name):
        """
        set all the fields of table to build dynamic queries.
        If fields parameter is given, then we only choose them (for example,
        it can be usefull to ignore columns with a default value set to date()
        or sysdate(). Those fields can be different due to different insert
        time)
        fields value is like where clause : "in (c1,c2,c3)" or "not in (c1,c2,
        c3)"

        example :  base on the table describe before
        then it will create a string like :

        in the template query :
        select hash_md5(xmlagg(XMLELEMENT(e,md5_concat,'').EXTRACT('//text()'))
        .GetClobVal()) as md5_concat,sum(nb) as numrow from (select hash_md5
        (concat) as md5_concat, 1 as nb from (select {fields} as concat,
        row_number() over (order by {order_by}) numrow from {schema}
        {tableName} where {wherest} order by {order_by}) q1 {stlimit}) r1
        this function is used to get " {fields} "
        """
        logging.debug(
            f"""{self.dbEngine}:set_fields from {self.schema}.{object_name}""")
        fieldst = ""
        datatype_st = ""
        server_field = ""

        sql = f"""SELECT column_name,'"'||column_name||'"' FROM all_tab_columns where
        (upper(owner), upper(table_name)) in ((upper('{self.schema}'),
        upper('{object_name}'))) ORDER BY column_id"""

        conn = self.connect()
        with conn.cursor() as curs:
            curs.execute(sql)
            rows = curs.fetchall()
            for row in rows:
                server_field = server_field + ',' + row[1]
                datatype = self.get_field_datatype(object_name,row[0])
                field = f"""nvl({row[0]},'')"""
                fieldst = fieldst + self.cast_field(field,datatype)
                datatype_st = datatype_st + datatype + '||'
        self.concatened_fields = fieldst.rstrip('||')
        self.datatype_list = datatype_st.rstrip('||')
        self.fields = server_field.lstrip(',')

    def set_pk(self):
        """
        set the primary key fields of the table
        used in dynamic query building for the order by clause
        example : will create a string  "EMPLOYEE_ID"
        """
        schema = self.schema
        tableName = self.tableName
        logging.debug(
            f"""{self.dbEngine}: __set_pk from {schema}.{tableName}""")
        sql = f"""SELECT cols.column_name,tc.column_id FROM
        all_constraints cons,
        all_cons_columns cols,
        all_tab_columns tc
        WHERE cols.table_name = upper('{tableName}') AND
        cols.owner= upper('{schema}') and
        cons.constraint_type = 'P' AND
        cons.constraint_name = cols.constraint_name AND
        cons.owner =  cols.owner and
        tc.owner=cols.owner and
        tc.table_name=cols.table_name and
        tc.column_name=cols.column_name
        ORDER BY cols.table_name, cols.position"""
        conn = self.connect()
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                rows = curs.fetchall()
                stp = ""
                stp_idx = ""
                for row in rows:
                    stp = stp + f"""{row[0]},"""
                    stp_idx = stp_idx + f"""{row[1]},"""
        self.pk = stp.rstrip(',')
        self.pk_idx = stp_idx.rstrip(',')

    def set_order_by(self):
        """
        set the primary key fields of the table
        used in dynamic query building for the order by clause
        example : will create a string  "EMPLOYEE_ID"
        """
        schema = self.schema
        tableName = self.tableName
        logging.debug(
            f"""{self.dbEngine}: __set_pk from {schema}.{tableName}""")
        sql = f"""SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = upper('{tableName}') AND  cols.owner= upper('{schema}') and cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner =  cols.owner ORDER BY cols.table_name, cols.position"""
        conn = self.connect()
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                rows = curs.fetchall()
                stp = ""
                for row in rows:
                    stp = stp + f"""{row[0]},"""
        self.order_by = stp.rstrip(',')

    def format_qry_last(self, start, stop):
        """
        use to build dynamic query of step 3. This query return a dataset.
        This dataset is compared to fetch differences

        example : base on the table describe before

        this query is used to compare dataset between server1 and server2

        Arguments:
          start {[int]} -- [between start]
          stop {[int]} -- [between stop]

        Returns:
          [string]

        in the template, this function replace all variable
        select hash_md5(xmlagg(XMLELEMENT(e,md5_concat,'').EXTRACT('//text()'))
        .GetClobVal()) as md5_concat,sum(nb) as numrow from
        ( select hash_md5(concat) as md5_concat, 1 as nb from
        (select {fields} as concat, row_number() over (order by {order_by})
        numrow from {schema}.{name} order by {order_by}) q1 {stlimit}) r1
        """
        stlimit = f"""where numrow between {start} and {stop}"""
        sql = f"""select {self.fields},
          hash_md5(concat) as md5_concat from
          (select {self.fields},
          {self.concatened_fields} as concat,
          row_number() over (order by {self.order_by}) numrow
          from {self.schema}.{self.viewName}
          order by {self.order_by}) q1
          {stlimit}
          """
        #  logging.debug(f"""format_qry_last {start} {stop}:{sql}""")
        return sql

    def format_qry(self):
        """
        use to build template for step 0
        this query will be used as template for all the process
        Example :
        in the repo database, you can change this query to remove fields
        or add where clause restriction.
        Returns:
          [string] -- template query
        """
        stlimit = "where numrow between {start} and {stop}"
        if self.fields != '':
            sql = f"""select
            hash_md5(nvl(xmlagg(XMLELEMENT(e,md5_concat,'').EXTRACT('//text()')).GetClobVal(),0)) as md5_concat,
            sum(nb) as numrow from (select hash_md5(concat) as md5_concat,
            1 as nb from (select {self.concatened_fields} as concat,
            row_number() over (order by 1) numrow
            from {self.schema}.{self.viewName}
            order by {self.order_by}) q1 {stlimit}) r1"""
        #    logging.debug(f"""{self.dbEngine}:format_qry : {sql}""")
        else:
            sql = ''
        return sql
