# syntax=docker/dockerfile:1
# Stage 1 builds the jar with a JDK. Stage 2 runs it on a JRE, without Maven or sources.

FROM eclipse-temurin:25-jdk AS build
WORKDIR /build
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY src src
# The cache mount keeps downloaded dependencies between builds; tests need Docker, so they run outside
RUN --mount=type=cache,target=/root/.m2 ./mvnw -q -B package -DskipTests
# Spring Boot splits the jar into layers, from the one that changes least to the one that changes most
RUN java -Djarmode=tools -jar target/*.jar extract --layers --launcher --destination extracted

FROM eclipse-temurin:25-jre
WORKDIR /app
RUN useradd --system --uid 1001 app
COPY --from=build /build/extracted/dependencies/ ./
COPY --from=build /build/extracted/spring-boot-loader/ ./
COPY --from=build /build/extracted/snapshot-dependencies/ ./
COPY --from=build /build/extracted/application/ ./
USER app
# The heap follows the container's memory limit instead of the node's memory
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "org.springframework.boot.loader.launch.JarLauncher"]
