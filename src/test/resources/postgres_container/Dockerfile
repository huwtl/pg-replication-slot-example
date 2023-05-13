FROM postgres:11.18-alpine

ENV WAL2JSON_COMMIT_ID=beb9293e9b9e867c7cc32d649e33f15528557d4d

# Compile the wal2json plugin from source and install
RUN apk add --no-cache --virtual .build-deps gcc clang llvm git make musl-dev pkgconf \
    && git clone https://github.com/eulerto/wal2json -b master --single-branch \
    && (cd /wal2json && git checkout $WAL2JSON_COMMIT_ID && make && make install) \
    && rm -rf wal2json \
    && apk del .build-deps

# Config sets suitable options for replication and selects the wal2json plugin
COPY postgresql.conf.sample /usr/local/share/postgresql/postgresql.conf.sample
