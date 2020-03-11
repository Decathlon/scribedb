import logging


class Table():
    """
    class table to represent a table in the repo structure
    example :
    all example are based on the table HR.EMPLOYEES
    """

    def __init__(self, cxString, schema, schemarepo):
        self.viewNamePrefix = schemarepo + '_'
        self.cxString = cxString
        self.schema = schema
        self.nbfields = 0
        self.order_by = None

    def cleanObjects(self, val):
        val = val.strip('.')
        val = val.strip('\n')
        val = val.strip('\t')
        return val

    def getengine(self):
        if self.cxString.startswith('postgresql'):
            val = "pg"
        else:
            val = "ora"
        return val

    def getNbFields(self):
        return self.nbfields

    def getDataTypeFields(self):
        return self.datatype_list

    def create(self, tableName, schema, select):
        if len(tableName) + len(self.viewNamePrefix) >= 30:
            lentbl = len(tableName) - len(self.viewNamePrefix)
        else:
            lentbl = len(tableName)
        self.viewName = self.viewNamePrefix + tableName[0:lentbl]
        self.tableName = tableName
        self.schema = schema
        self.drop_view()
        if select is None:
            self.set_fields(tableName)
            self.set_pk()
            self.set_order_by()
            select = f"""select {self.fields} from {schema}.{tableName}
            order by {self.order_by}"""
        else:
            self.order_by = select.split('order by')[1]
            tmp_order = self.order_by.split(',')
            colt = ''
            for result_order in tmp_order:
                #col = self.sanitize_name(result_order)
                col = result_order
                colt = colt + col + ','
            colt = colt.rstrip(',')
            self.order_by = colt[:-1]

        self.create_view(schema,self.viewName,select)
        self.set_fields(self.viewName)
        self.nbfields = len(self.fields.split(','))
        self.set_numrows(self.viewName)
        self.select = select

    def drop_view(self):
        """
        see in herited classe for details
        """
        raise NotImplementedError

    def set_fields(self):
        """
        see in herited classe for details
        """
        raise NotImplementedError

    def set_pk(self):
        """
        see in herited classe for details
        """
        raise NotImplementedError

    def set_order_by(self):
        """
        see in herited classe for details
        """
        raise NotImplementedError

    def sanitize_name(self,name):
        """
        see in herited classe for details
        """
        sanitized_column = name.split('.')
        if len(sanitized_column) > 1:
            s_column = sanitized_column[1]
        else:
            s_column = sanitized_column[0]
        sanitized_column = s_column.split('(')
        if len(sanitized_column) > 1:
            s_column = sanitized_column[1]
        else:
            s_column = sanitized_column[0]
        s_column = s_column.replace('"','')
        s_column = s_column.replace(')','')
        s_column = s_column.replace('\n','')
        s_column = s_column.rstrip(' ')
        s_column = s_column.lstrip(' ')
        return s_column

    def create_view(self, schema, viewName, select):
        """
        create temporary view to be able to get the datatype for cast
        drop the view if exists
        """
        sql = f"""create or replace view {schema}.{viewName} as {select}"""
        conn = self.connect()
        with conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(sql)
                except conn.DatabaseError as exc:
                    error, = exc.args
                    logging.error(
                        f"""{self.dbEngine}: error executing {sql} : {error}""")

    def get_tablelist(self, qry_include_table):
        """
        see in herited classe for details
        """
        pass

    def set_numrows(self, viewName):
        """
        set the table numrows value. Use in server1_rows and server2_rows
        fields of repo
        example :
        numrows = select count(*) from "HR"."EMPLOYEES"
        """

        logging.debug(
            f"""{self.dbEngine}: select count(*) as nb from {self.schema}.{viewName}""")
        sql = f"""select count(*) as nb from {self.schema}.{viewName}"""
        conn = self.connect()
        with conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(sql)
                except conn.DatabaseError as exc:
                    error, = exc.args
                    logging.error(
                        f"""{self.dbEngine}: error executing {sql} : {error}""")
                    self.numrows = 0
                else:
                    row = curs.fetchone()
                    self.numrows = row[0]
                    logging.debug(
                        f"""{self.dbEngine}: numrows={self.numrows}""")
        return None
