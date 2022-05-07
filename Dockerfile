FROM python:3.9-bullseye

LABEL org.opencontainers.image.authors="oss@decathlon.com"

RUN apt-get update && apt-get install -y --no-install-recommends alien libaio1 wget && \
    wget https://download.oracle.com/otn_software/linux/instantclient/185000/oracle-instantclient18.5-basic-18.5.0.0.0-3.x86_64.rpm && \
    wget https://download.oracle.com/otn_software/linux/instantclient/185000/oracle-instantclient18.5-devel-18.5.0.0.0-3.x86_64.rpm && \
    wget https://download.oracle.com/otn_software/linux/instantclient/185000/oracle-instantclient18.5-sqlplus-18.5.0.0.0-3.x86_64.rpm && \
    alien -i oracle-instantclient18.5-basic-18.5.0.0.0-3.x86_64.rpm && \
    alien -i oracle-instantclient18.5-devel-18.5.0.0.0-3.x86_64.rpm && \
    alien -i oracle-instantclient18.5-sqlplus-18.5.0.0.0-3.x86_64.rpm && \
    rm -f oracle-instantclient18.5-basic-18.5.0.0.0-3.x86_64.rpm && \
    rm -f oracle-instantclient18.5-devel-18.5.0.0.0-3.x86_64.rpm && \
    rm -f oracle-instantclient18.5-sqlplus-18.5.0.0.0-3.x86_64.rpm

ENV LD_LIBRARY_PATH="/usr/lib/oracle/18.5/client64/lib:${LD_LIBRARY_PATH}"
ENV PATH=$PATH:/usr/lib/oracle/18.5/client64/bin

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY main.py /
COPY scribedb /scribedb

ENTRYPOINT ["python3","/main.py"]
