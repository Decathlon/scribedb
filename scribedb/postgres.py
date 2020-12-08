import psycopg2
import logging
import socket
from rdbms import Table as TableRdbms


class Table(TableRdbms):

    dbEngine = "pgsql"

    """
    class table to represent a table in the repo structure

    example :
    all example are based on the table HR.EMPLOYEES
    """

    def __init__(self, cxString, schema,schemarepo):
        TableRdbms.__init__(self,cxString,schema,schemarepo)
        self.conn = self.connect()

    def connect(self):
        """
        return a database connection to postgres before executing query
        we use this method, to be able to set particular session's parameters
        before execute
        Arguments:
            cxString {[string]} -- [connection string]
        Returns:
            [connection object]
        """
        conn = psycopg2.connect(self.cxString)
        s = socket.fromfd(conn.fileno(),socket.AF_INET, socket.SOCK_STREAM)
        # Enable sending of keep-alive messages
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Time the connection needs to remain idle before start sending
        # keepalive probes
    #    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
        # Time between individual keepalive probes
    #    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 1)
        # The maximum number of keepalive probes should send before dropping
        # the connection
    #    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        cur = conn.cursor()
        cur.execute(f"""SET search_path TO {self.schema}""")
        return conn

    def get_tablelist(self, qry_include_table):
        """

        return the table list to compute from database catalog

        Arguments:
            qry_include_table {[type]} -- where clause to include ex : and
            {qry_include_table} default = true

        Returns:
            [dataset] -- [list tables]
        """
        logging.debug(
            f"""{self.dbEngine}: get_tablelist for {qry_include_table}""")
        sql = f"""select cls.relname as table_name,to_char(cls.reltuples,'99999999999999')
                    from pg_class cls inner
                    join pg_namespace nsp on nsp.oid = cls.relnamespace
                    join information_schema.tables t on
                    cls.relname = t.table_name
                    and nsp.nspname = t.table_schema
                    where nsp.nspname not in ('information_schema',
                    'pg_catalog')
                    --and nsp.nspname not like 'pg_toast%'
                    and nsp.nspname = '{self.schema}'
                    and cls.relkind = 'r'
                    and {qry_include_table}
                    order by cls.reltuples,nsp.nspname, cls.relname"""
        conn = self.connect()
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                rows = curs.fetchall()
        return rows

    def drop_view(self):
        """
        see in herited classe for details

        drop temporary view to be able to get the datatype for cast
        drop the view if exists
        """
        logging.debug(
            f"""{self.dbEngine}: drop view for {self.schema}.{self.viewName}""")
        sql = f"""drop view if exists {self.schema}.{self.viewName}"""
        conn = self.conn
        with conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(sql)
                except Exception as e:
                    error, = e.args
                    if error.find('does not exists') == 0:
                        logging.error(
                            f"""{self.dbEngine}:error executing {sql} : {error}""")
        sql = f"""drop view if exists {self.schema}.{self.viewName}_c"""
        conn = self.conn
        with conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(sql)
                except Exception as e:
                    error, = e.args
                    if error.find('does not exists') == 0:
                        logging.error(
                            f"""{self.dbEngine}:error executing {sql} : {error}""")

    def get_field_datatype(self, object_name, column):
        """ used during dynamic query building, to eventually add quote or not.
        can be used for orale or pgsql.
        Infos are from information_schema.columns or all_tab_columns

        Arguments:
            column {string} -- the column name you need the type

        example
        input : EMPLOYEES_ID
        output : VARCHAR2(25 BYTE)

        Returns:
            string    datatype
        """
        s_col = self.sanitize_name(column)
        sql = f"""SELECT data_type FROM information_schema.columns where
        (table_schema, table_name, column_name) = ('{self.schema}','{object_name}','{s_col}')"""
        sql = sql.lower()
        conn = self.connect()
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                row = curs.fetchone()
                if row is None:
                    logging.error(f"""{self.dbEngine}:object_name:#{object_name}# column #
                    {column}# not found sql:#{sql}#""")
        return row[0]

    def set_fields(self, object_name):
        """
        set all the fields of table to build dynamic queries.
        If fields parameter is given, then we only choose them (for example,
        it can be usefull to ignore columns with a default value set to date()
        or sysdate(). Those fields can be different due to different insert
        time)
        fields value is like where clause : "in (c1,c2,c3)" or "not in (c1,c2,
        c3)"

        example :    base on the table describe before
        then it will create a string like :

        in the template query :
        select md5(string_agg(r1.md5_concat,'')),sum(nb) as numrow from
        (select md5(concat) as md5_concat, 1 as nb from (select {fields} as
        concat from {schema}.{tableName} where {wherest} order by
        {self.order_by} {stlimit}) q1) r1

        this function is used to get " {fields} "

        """
        logging.debug(
            f"""{self.dbEngine}:get_fields from {self.schema}.{object_name}""")
        fieldst = ""
        datatype_st = ""
        server_field = ""
        myfields = []
        sql = f"""SELECT '"'||column_name||'"' FROM information_schema.columns where
        (lower(table_schema), lower(table_name)) = (lower('{self.schema}'),
        lower('{object_name}')) ORDER BY ordinal_position"""
        conn = self.connect()
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                rows = list(curs.fetchall())
                for row in rows:
                    rv = row[0]
                    myfields.append((row[0].strip('"')).upper())
                    server_field = server_field + ',' + rv
                    datatype = self.get_field_datatype(object_name,rv)
                    datatype_st = datatype_st + datatype + '||'
                    self.datatype_list = datatype_st
                    if ("timestamp" in datatype) or ("date" in datatype):
                        fieldst = fieldst + \
                            f""" coalesce(to_char({rv},'YYYY-MM-DD HH24:MI:SS'),'')||'|'||"""
                    elif ("double precision" in datatype) or ("numeric" in datatype):
                        fieldst = fieldst + \
                            f""" coalesce(ltrim(rtrim(rtrim(to_char({rv},'999999999999990.999999'),'0'),'.'),' '),'')||'|'||"""
                    elif "char" in datatype:
                        """
                        in oracle, fields in char(10),
                        if this field is not full, it is completed with spaces
                        but not in pgsql.
                        That's why i remove them on oracle and pgsql
                        """
                        fieldst = fieldst + \
                            f""" coalesce(rtrim({rv}),'')||'|'||"""
                    else:
                        fieldst = fieldst + \
                            f""" coalesce({rv}::text,'')||'|'||"""
                self.tup_fields = tuple(myfields)
        self.concatened_fields = fieldst.rstrip('||')
        self.datatype_list = datatype_st.rstrip('||')
        self.fields = server_field.lstrip(',')

    def set_pk(self):
        """
        set the primary key fields of the table
        used in dynamic query building for the order by clause
        example : will create a string    "EMPLOYEE_ID"
        """
        schema = self.schema
        tableName = self.tableName
        logging.debug(f"""{self.dbEngine}: get_pk from {schema}.{tableName}""")
        sql = f"""SELECT c.column_name,c.ordinal_position FROM information_schema.table_constraints
        JOIN information_schema.key_column_usage USING
        (constraint_catalog, constraint_schema, constraint_name,table_catalog,
        table_schema, table_name) 
        join information_schema.columns c using (table_catalog,table_schema,table_name,column_name)
        WHERE constraint_type = 'PRIMARY KEY'
        AND (table_schema, table_name) = ('{schema}', '{tableName}') ORDER BY
        ordinal_position"""
        stp = ""
        stp_idx = ""
        conn = self.connect()
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                rows = curs.fetchall()
                for row in rows:
                    stp = stp + f"""{row[0]},"""
                    stp_idx = stp_idx + f"""{row[1]},"""
        self.pk = stp.rstrip(',')
        self.pk_idx = stp_idx.rstrip(',')

    def set_order_by(self):
        """
        set the primary key fields of the table
        used in dynamic query building for the order by clause
        example : will create a string    "EMPLOYEE_ID"
        """
        schema = self.schema
        tableName = self.tableName
        logging.debug(
            f"""{self.dbEngine}: get_order_by from {schema}.{tableName}""")
        sql = f"""SELECT column_name FROM information_schema.table_constraints
        JOIN information_schema.key_column_usage USING
        (constraint_catalog, constraint_schema, constraint_name,table_catalog,
        table_schema, table_name) WHERE constraint_type = 'PRIMARY KEY'
        AND (table_schema, table_name) = ('{schema}', '{tableName}') ORDER BY
        ordinal_position"""
        field = ""
        conn = self.connect()
        with conn.cursor() as curs:
            curs.execute(sql)
            rows = curs.fetchall()
            stp = ""
            for row in rows:
                field = row[0]
                field_type = self.get_field_datatype(tableName,field)
                if "char" in field_type:
                    field = field + " collate \"POSIX\" "
                stp = stp + f"""{field},"""
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
            [string] --

        in the template, this function replace all variable
        select {fields},
            md5(concat) as md5_concat from
            (select {self.fields},
            {fields} as concat,
            row_number() over (order by 1) as numrow
            from {schema}.{tableName} where {where}) q1
            {stlimit}
        """
        stlimit = f"""where numrow between {start} and {stop}"""
        sql = f"""select {self.fields},
            md5(concat) as md5_concat from
            (select {self.fields},
            {self.concatened_fields} as concat,
            row_number() over (order by {self.order_by}) as numrow
            from {self.schema}.{self.viewName}) q1
            {stlimit}
            """
        logging.debug(
            f"""{self.dbEngine}: format_qry_last {start} {stop}:{sql}""")
        return sql

    def format_qry(self):
        """
        use to build template for step 0
        this query will be used as template for all the process

        Example :

        in the repo database, you can change this query to remove fields or
        add where clause restriction.

        Returns:
            [string] -- template query

        """

        stlimit = "limit {limit} offset {offset}"
        if self.fields != '':
            sql = f"""select md5(string_agg(r1.md5_concat,'')),sum(nb) as numrow
            from (select md5(concat) as md5_concat,
            1 as nb from
            (select {self.concatened_fields} as concat
            from {self.schema}.{self.viewName}
            order by {self.order_by} {stlimit}) q1) r1"""
            # logging.critical(
            #    f"""{self.dbEngine}:format_qry : {sql}""")
        else:
            sql = ''
        return sql
