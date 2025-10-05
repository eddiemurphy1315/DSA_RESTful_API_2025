FROM ballerina/ballerina:2201.9.0

WORKDIR /app

COPY . .

RUN bal build

EXPOSE 9092

CMD ["bal", "run", "target/bin/ticketing_service.jar"]