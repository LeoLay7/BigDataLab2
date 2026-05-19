FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /build

COPY pom.xml ./
COPY src ./src

RUN mvn -DskipTests package

FROM jupyter/pyspark-notebook:spark-3.5.0

USER root
WORKDIR /home/jovyan/work

COPY --from=build /build/target/spark-module-1.0-SNAPSHOT.jar /home/jovyan/work/spark-module-1.0-SNAPSHOT.jar


USER jovyan

