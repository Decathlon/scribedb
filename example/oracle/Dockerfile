FROM wnameless/oracle-xe-11g-r2:latest
MAINTAINER Pierre-Marie Petit

ADD files/hr.sql /docker-entrypoint-initdb.d/

# 1521: Oracle listener
# 5500: Oracle Enterprise Manager (EM) Express listener.
EXPOSE 1521