FROM openjdk
FROM maven
WORKDIR /app
COPY src /app/src
COPY pom.xml /app
RUN mvn clean package
CMD java -jar /app/target/udp-listener-1.0-SNAPSHOT.jar
EXPOSE 5140
