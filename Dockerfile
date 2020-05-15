FROM oraclelinux:7-slim

LABEL maintainer "oss@decathlon.com"

RUN  curl -o /etc/yum.repos.d/public-yum-ol7.repo https://yum.oracle.com/public-yum-ol7.repo && \
  yum -y install https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
  yum-config-manager --enable ol7_oracle_instantclient && \
  yum -y install oracle-instantclient18.3-basic oracle-instantclient18.3-devel oracle-instantclient18.3-sqlplus postgresql96 && \
  echo /usr/lib/oracle/18.3/client64/lib > /etc/ld.so.conf.d/oracle-instantclient18.3.conf && \
  ldconfig && \
  yum install -y yum-utils && \
  yum-config-manager --enable *EPEL && \
  yum install -y python36 && \
  yum install -y python36-pip && \  
  rm -rf /var/cache/yum      

ENV PATH=$PATH:/usr/lib/oracle/18.3/client64/bin
ENV LD_LIBRARY_PATH=usr/lib/oracle/18.3/client64/lib

COPY requirements.txt .
RUN pip3.6 install --no-cache-dir -r requirements.txt

COPY scribedb/*.py /

CMD ["python3.6","./scribedb.py"]
