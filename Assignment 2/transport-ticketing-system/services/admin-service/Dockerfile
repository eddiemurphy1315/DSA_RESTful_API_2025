FROM ballerina/ballerina:2201.9.0

WORKDIR /app

COPY . .

RUN bal build

EXPOSE 9095

CMD ["bal", "run", "target/bin/admin_service.jar"]