# Available env. variables 

- **cxstring1** Connection string to server1 
- **cxstring2** Connection string to server2 
- **cxrepo** Connection string to server Repo 
- **schema1** schema name on server1 Ex : hr
- **schema2** schema name on server2 Ex :HR
- **schemarepo** repository schema name Ex : diffhr
- **log** log level (debug,info,warn)
- **maxdiff** raise the first "maxdiff" differences default : 50
- **qry_include_table** where clause to select table to include in the check Ex : "table_name in ('employees')" or "true" (for all tables)
be carefull, in oracle objects are in uppercase, so even if the filter is the same in server1 and server2, if server2 is an oracle, you have to change the table_name in uppercase.
- **high_limit** threshold to manage compare data Ex  : 120000
it is the size of the 1st step bucket. a table of 1200000 rows will be splitted in 10 buckets of 120000. each of them are computed on each server (there is no network pressure, but a high cpu pressure on server1 and server2). If each buckets are ok (data are the same) then it stops. If one of them is not ok, then this 120000 rows bucket will be splitted in 3 buckets of 40001. If one of them is not ok, then the 2 datasets (one from server1 and one from server2) are downloaded over the network, to be compared with a minus operation (high network pressure and cpu pressure on the docker image itself, but no cpu pressure on server1 and server2).
- **low_limit** threshold to manage compare data Ex : 40001
40001 is the number of rows we decide to download over the network, to compare in the docker image with a minus operation. 
