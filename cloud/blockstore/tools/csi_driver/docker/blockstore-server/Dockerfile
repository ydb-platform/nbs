FROM ubuntu:20.04

WORKDIR /

COPY /nbsd /usr/bin/nbsd
COPY /blockstore-client /usr/bin/blockstore-client

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -yqq update
RUN apt-get -yqq upgrade
RUN apt-get -yqq install dnsutils
RUN apt-get -yqq install telnet
RUN apt-get -yqq install netcat-openbsd
RUN apt-get -yqq install iputils-ping
RUN apt-get -yqq install gdb
RUN apt-get -yqq install python3-minimal
RUN apt-get -yqq install jq
RUN apt-get -yqq clean all

ENTRYPOINT ["/usr/bin/nbsd"]
