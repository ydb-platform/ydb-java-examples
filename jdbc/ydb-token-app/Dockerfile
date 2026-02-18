FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /workspace

COPY . .
RUN mvn -B -pl jdbc/ydb-token-app -am dependency:go-offline
RUN mvn -B -pl jdbc/ydb-token-app -am clean package org.springframework.boot:spring-boot-maven-plugin:repackage -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app

COPY --from=build /workspace/jdbc/ydb-token-app/target/ydb-token-app-1.1.0-SNAPSHOT.jar /app/app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
