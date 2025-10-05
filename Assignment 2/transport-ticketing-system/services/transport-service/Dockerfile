FROM ballerina/ballerina:2201.9.0

WORKDIR /app

COPY . .

RUN bal build

EXPOSE 9091

CMD ["bal", "run", "target/bin/transport_service.jar"]