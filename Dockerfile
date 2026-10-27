FROM eclipse-temurin:21-jdk-jammy AS build-env
WORKDIR /app
COPY . .
RUN ./mvnw clean package

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build-env /app/target/mock-server-0.0.1-SNAPSHOT.jar ./

CMD ["java", "-jar", "mock-server-0.0.1-SNAPSHOT.jar"]
