# syntax=docker/dockerfile:1
# click-aggregator image: Flink 2.2.1 (scala 2.12, java 17) + Kafka/JDBC connectors + the SQL job.
#   docker build -t click-aggregator pipelines/click-aggregator            (runs the unit tests)
#   docker build --build-arg SKIP_TESTS=true ...                           (faster rebuilds)
# Versions are pinned together (see job/pom.xml and README.md).

FROM maven:3.9.16-eclipse-temurin-17 AS build
WORKDIR /src
COPY job/pom.xml .
# Retried: right after Docker Desktop starts, the first request to Maven Central often fails, and
# Maven then reports it as "No plugin found for prefix 'dependency'" (seen twice on a fresh `make ci`).
RUN --mount=type=cache,target=/root/.m2 \
    for i in 1 2 3 4; do \
      mvn -B -q dependency:resolve dependency:resolve-plugins && exit 0; \
      echo "maven pre-fetch failed (attempt $i), retrying in $((i * 5))s" >&2; sleep $((i * 5)); \
    done; exit 1
COPY job/src ./src
ARG SKIP_TESTS=false
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B verify -DskipTests=${SKIP_TESTS}

FROM flink:2.2.1-scala_2.12-java17
# the shaded job jar (SQL + runner + UDF + Kafka/JDBC connectors + driver) -> usrlib (application mode)
COPY --from=build --chown=flink:flink /src/target/click-aggregator.jar /opt/flink/usrlib/click-aggregator.jar
COPY --chown=flink:flink --chmod=0755 docker/sdl-entrypoint.sh /opt/flink/bin/sdl-entrypoint.sh
