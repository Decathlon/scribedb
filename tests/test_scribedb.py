
import unittest
import logging
import warnings
import psycopg2

from scribedb import scribedb,oracle,postgres

class TestCompare(unittest.TestCase):

    def ignore_warnings(test_func):
        def do_test(self, *args, **kwargs):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                test_func(self, *args, **kwargs)
        return do_test

    @ignore_warnings
    def test_compare(self):
        high_limit = 6
        low_limit = 4
        cxstring2 = "hr/hr@localhost:1521/xe"
        cxstring1 = "postgresql://postgres:postgres@localhost:5432/hr"
        cxrepo = "postgresql://postgres:postgres@localhost:5432/hr"
        schema1 = "hr"
        schema2 = "hr"
        schemarepo = "hrdiff2"
        qry_include_table = "lower(table_name) not in ('employees')"
        repo = scribedb.Repo(cxrepo,
                             schemarepo,
                             cxstring1,
                             cxstring2,
                             schema1,
                             schema2)
        repo.dropSchema()
        repo.create()
        if cxstring1.startswith('postgresql'):
            table1 = postgres.Table(cxstring1,schema1)
        else:
            table1 = oracle.Table(cxstring1,schema1)

        if cxstring2.startswith('postgresql'):
            table2 = postgres.Table(cxstring2,schema2)
        else:
            table2 = oracle.Table(cxstring2,schema2)

        listTables = table1.get_tablelist(qry_include_table)
        for table in listTables:
            tablename = table[0]
            table1.create(tablename,None)
            table2.create(tablename,None)
            repo.insert_table_diff(table1,table2)
            step = 0
            while step < 2:
                step = step + 1
                repo.split(table1,table2,step,high_limit,low_limit)
                check_md5 = repo.compute_md5(table1,table2)
                if check_md5.result == '':
                    break
                if check_md5.result == 'nok':
                    if (step == 1 and check_md5.numrows < low_limit) \
                       or (step == 2):
                        repo.compute_diffrowset(table1,table2)
                        break
            repo.update_table_result(table1)
            table1.drop_view();
            table2.drop_view();

        sql = f"""select count(*) from {schemarepo}.tablediff
        where result = 'nok'"""
        conn = psycopg2.connect(cxrepo)
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                row = curs.fetchone()
                result = row[0]

        self.assertEqual(result, 0, f"""select count(*) from {schemarepo}.tablediff
        where result = 'nok'  = > Should be 0""")


if __name__ == '__main__':
    unittest.main()
