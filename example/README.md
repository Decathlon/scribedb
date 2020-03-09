## Description

the example includes 3 dockers images:

* scribedb
* 2 databases
  * oracle XE
To build the oracle XE image, [Download](https://www.oracle.com/technetwork/database/database-technologies/express-edition/downloads/index.html) the RPM from Oracle Technology Network and save to folder. We will assume it is in `scribedb/example/oracle/files/oracle-database-xe-18c-1.0-1.x86_64.rpm`.
  * postgres
  
each database comes with an embedded HR schema, both identical.

Scribedb will compare HR schema between postgresql and oracle.

## Running the example

```bash
cd scribedb/example
docker-compose build
docker-compose up
```

it waits for oracle and posgresql image to be available, before starting.

The script perform the control in 2 executions : 
After the 1st one, the table TableDiff is initialized. You can then modify some parameters to reflect your needs.

When you have tuned the parameters, you can run the same script a second time. Then it will seek for differences.

```bash
docker-compose up
```

check that everything is ok in oracle and pgsql databases.

```bash
sqlplus -s hr/hr@localhost:1521/XE <<EOF
select count(*) from hr.regions;
EOF
```

```log
  COUNT(*)
----------
  4

1 row selected.

```

```bash
export PGPASSWORD=postgres
psql -p 5432 -h localhost -U postgres -d hr<<EOF
select count(*) from hr.regions
EOF
```

```log
 count
-------
     4
(1 row)
```


in the 3 cases bellow, we compare the HR schema. As you can see high_limt and low_limit are set to very low values. It is just for testing and to be sure all steps are executed. On a real experience, we set them to high_limit=120000 and low_limit=40001.

#### use case N°1

the hr schema was migrate using ora2pg, we must compare the whole schema.

```bash
      - high_limit=6  
      - low_limit=4
      - cxstring2=hr/hr@oradb:1521/xe
      - cxstring1=postgresql://postgres:postgres@pgdb:5432/hr
      - cxrepo=postgresql://postgres:postgres@pgdb:5432/hr
      - schema1=hr
      - schema2=hr
      - schemarepo=diffhr
      - log=debug
      - qry_include_table="true"
```

qry_include_table = "true" is the where clause for getting tables to compare. 

```sql
select lower(table_name),num_rows from all_all_tables 
where upper(owner)=upper('{self.schema2}') 
and {qry_include_table} order by num_rows,table_name
```

#### use case N°2

only the table employee without any filters on column and rows. We want to compare only column "first_name" and "last_name".

Start the script with those variables:

```bash
      - high_limit=6  
      - low_limit=4
      - cxstring2=hr/hr@oradb:1521/xe
      - cxstring1=postgresql://postgres:postgres@pgdb:5432/hr
      - cxrepo=postgresql://postgres:postgres@pgdb:5432/hr
      - schema1=hr
      - schema2=hr
      - schemarepo=diffhr
      - log=debug
      - qry_include_table="table_name in ('employees')"
```

after the first launch, do not restart the script. Query the table diffhr.tablediff
for rows with department_id=100, and create the query.

server1_sql :

```sql
SELECT first_name,last_name from hr.employees
```

server2_sql :

```sql
SELECT first_name,last_name from hr.employees
```


#### use case N°3

only the table employee with filter on column and rows. let says that there is a transformation between oracle and pgsql :

table : employees => employees_new
column : first_name => fname
column : lastname => lname
column : salary => salary * 2

We want to compare only columns, but salary is twice more in pgsql than in oracle.

```sql
 column "first_name,last_name,salary" for rows with department_id=100
```

Start the script with those variables:

```bash
      - high_limit=6  
      - low_limit=4
      - cxstring2=hr/hr@oradb:1521/xe
      - cxstring1=postgresql://postgres:postgres@pgdb:5432/hr
      - cxrepo=postgresql://postgres:postgres@pgdb:5432/hr
      - schema1=hr
      - schema2=hr
      - schemarepo=diffhr
      - log=debug
      - qry_include_table="table_name in ('employees')"
```

after the first launch, do not restart the script. Query the table diffhr.tablediff
for rows with department_id=100, and create the query.

to be sure we compare the same thing, we must double the salary in oracle table.

server1_sql:

```sql
SELECT first_name,last_name,2*salary from hr.employees where department_id=100
```

server2_sql :

```sql
SELECT fname,lname,salary from hr.employees_new where department_id=100
```

#### restart a comparaison

to restart a comparaison and keep your step 0 modifications, you can just:

```sql
delete from diffhr.tablediff where step>0;
truncate table diffhr.rowdiff;
```
