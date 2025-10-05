FROM ballerina/ballerina:2201.9.0

WORKDIR /app

COPY . .

RUN bal build

EXPOSE 9094

CMD ["bal", "run", "target/bin/notification_service.jar"]