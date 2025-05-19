FROM maven:3.8.5-openjdk-11 as builder
WORKDIR /build
COPY . .
RUN mvn clean package -DskipTests

FROM flink:1.15.0-scala_2.12
COPY --from=builder /build/target/clickstream-job-1.0.jar /opt/flink/usrlib/clickstream-job.jar
