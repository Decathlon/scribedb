FROM oraclelinux:7-slim

RUN  curl -o /etc/yum.repos.d/public-yum-ol7.repo https://yum.oracle.com/public-yum-ol7.repo && \
  yum -y install https://download.postgresql.org/pub/repos/yum/9.6/redhat/rhel-7-x86_64/pgdg-oraclelinux96-9.6-3.noarch.rpm && \
  yum-config-manager --enable ol7_oracle_instantclient && \
  yum -y install oracle-instantclient18.3-basic oracle-instantclient18.3-devel oracle-instantclient18.3-sqlplus postgresql96 && \
  echo /usr/lib/oracle/18.3/client64/lib > /etc/ld.so.conf.d/oracle-instantclient18.3.conf && \
  ldconfig && \
  yum install -y yum-utils && \
  yum-config-manager --enable *EPEL && \
  yum install -y python36 && \
  yum install -y python36-pip && \  
  pip3.6 install cx_Oracle && \
  pip3.6 install psycopg2-binary && \     
  rm -rf /var/cache/yum      

ENV PATH=$PATH:/usr/lib/oracle/18.3/client64/bin
ENV LD_LIBRARY_PATH=usr/lib/oracle/18.3/client64/lib

COPY scribedb/*.py /
COPY example/scribedb/wait-for-postgres-and-oracle.sh /
COPY example/scribedb/scripts/*.sql /

CMD ["python3","./scribedb.py"]
#CMD ["sleep","5000000"]

