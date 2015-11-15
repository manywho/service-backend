FROM java:8

EXPOSE 8080

CMD ["java", "-jar", "Backend-1.0-SNAPSHOT.jar"]

WORKDIR /app

ADD . ./
