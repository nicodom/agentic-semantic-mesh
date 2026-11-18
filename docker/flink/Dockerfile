# Flink with the connectors the SQL jobs need (ADR 0016 for versions).
# Each jar is pinned by SHA-256: the build fails if Maven Central ever serves different bytes.
FROM flink:2.2.1-java17

ARG MAVEN=https://repo1.maven.org/maven2

# Flink's Kafka connector: lets Flink SQL read and write Kafka topics. Used by every job.
# (Not to be confused with Iceberg's Kafka Connect sink, which this project does not use; ADR 0018.)
ADD --chown=flink:flink \
    --checksum=sha256:5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616 \
    ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
    /opt/flink/lib/

# Avro format that reads and writes Schema Registry framing (`avro-confluent`, ADR 0020).
ADD --chown=flink:flink \
    --checksum=sha256:9f37aacbbcf7c7b5dd34dd673b16f83c4f18787337cea8e6378c71a7e7405c9a \
    ${MAVEN}/org/apache/flink/flink-sql-avro-confluent-registry/2.2.1/flink-sql-avro-confluent-registry-2.2.1.jar \
    /opt/flink/lib/

# S3 file system for checkpoints and savepoints in RustFS (ADR 0023). It ships in the image's
# opt/ folder; Flink loads it as a plugin, isolated from the jars in lib/. Presto's is the one
# Flink recommends for checkpoints.
RUN mkdir /opt/flink/plugins/s3-fs-presto \
    && cp /opt/flink/opt/flink-s3-fs-presto-2.2.1.jar /opt/flink/plugins/s3-fs-presto/

# Iceberg catalog and sink, used only by the materialization job (ADR 0018).
ADD --chown=flink:flink \
    --checksum=sha256:c79bbee8792b1115c321e4be99fab3fb36db0bf8fa2bba24b481e6da14416de3 \
    ${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-2.2/1.12.0/iceberg-flink-runtime-2.2-1.12.0.jar \
    /opt/flink/lib/

# AWS SDK for Iceberg's S3FileIO, which talks to RustFS.
ADD --chown=flink:flink \
    --checksum=sha256:dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 \
    ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar \
    /opt/flink/lib/

# Iceberg's Flink catalog references Hadoop's Configuration class even with S3FileIO and a REST
# catalog (no Hadoop in use). The shaded client jars provide it without clashing with Flink's
# own dependencies. 3.4.3 is the version Iceberg 1.12.0 builds against.
ADD --chown=flink:flink \
    --checksum=sha256:6443332781f26a7199009d9517cd1eb013fbf54ca1c9759a2a9e487542b1d52a \
    ${MAVEN}/org/apache/hadoop/hadoop-client-api/3.4.3/hadoop-client-api-3.4.3.jar \
    /opt/flink/lib/
ADD --chown=flink:flink \
    --checksum=sha256:bd20267b0555766ae84697dd888f074ca326d8e2ec3c0008928a6ac2966c67de \
    ${MAVEN}/org/apache/hadoop/hadoop-client-runtime/3.4.3/hadoop-client-runtime-3.4.3.jar \
    /opt/flink/lib/
