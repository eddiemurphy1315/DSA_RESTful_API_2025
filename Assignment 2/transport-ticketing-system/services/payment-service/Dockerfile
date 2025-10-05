FROM ballerina/ballerina:2201.9.0

WORKDIR /app

COPY . .

RUN bal build

EXPOSE 9093

CMD ["bal", "run", "target/bin/payment_service.jar"]