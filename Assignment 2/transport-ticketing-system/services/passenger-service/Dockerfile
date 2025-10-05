FROM ballerina/ballerina:2201.9.0

WORKDIR /app

COPY . .

RUN bal build

EXPOSE 9090

CMD ["bal", "run", "target/bin/passenger_service.jar"]