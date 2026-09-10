# Two stages so the shipped image carries a JRE and a jar, not a full toolchain.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
# Copy the pom first so dependency resolution is cached independently of source
# changes — an edit to a Java file does not re-download the internet.
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q clean package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/target/kafka-ordering-demo-1.0.0.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
