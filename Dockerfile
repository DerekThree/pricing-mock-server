FROM eclipse-temurin:21-jdk-jammy AS build-env
WORKDIR /app/pricing-mock-server
COPY pricing-mock-server/ .
COPY pricing-engine/src/main/resources/db/account/ \
    /app/pricing-engine/src/main/resources/db/account/
COPY pricing-engine/src/main/resources/db/account-postgresql/ \
    /app/pricing-engine/src/main/resources/db/account-postgresql/
COPY pricing-engine/src/main/resources/db/recommendation/ \
    /app/pricing-engine/src/main/resources/db/recommendation/
RUN ./mvnw clean package

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build-env /app/pricing-mock-server/target/mock-server-0.0.1-SNAPSHOT.jar ./

CMD ["java", "-jar", "mock-server-0.0.1-SNAPSHOT.jar"]
