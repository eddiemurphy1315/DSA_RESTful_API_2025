//Excellence
import ballerina/http;
import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;
import ballerina/log;
import ballerinax/kafka;
import ballerina/uuid;
import ballerina/time;
import ballerina/random;

configurable string dbHost = "payment-db";
configurable int dbPort = 5432;
configurable string dbUser = "payment_user";
configurable string dbPassword = "payment_pass";
configurable string dbName = "payment_db";
configurable string kafkaBootstrapServers = "kafka:29092";

final postgresql:Client dbClient = check new (
    host = dbHost,
    port = dbPort,
    username = dbUser,
    password = dbPassword,
    database = dbName
);

kafka:ProducerConfiguration producerConfig = {
    clientId: "payment-service-producer",
    acks: "all",
    retryCount: 3
};

final kafka:Producer kafkaProducer = check new (kafkaBootstrapServers, producerConfig);

kafka:ConsumerConfiguration consumerConfig = {
    groupId: "payment-service-group",
    topics: ["ticket-requests"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: true
};

listener kafka:Listener kafkaConsumer = new (kafkaBootstrapServers, consumerConfig);

type Payment record {|
    string id?;
    string ticket_id;
    string user_id;
    decimal amount;
    string status;
    string payment_method?;
    string created_at?;
|};

function initDatabase() returns error? {
    _ = check dbClient->execute(`
        CREATE TABLE IF NOT EXISTS payments (
            id VARCHAR(255) PRIMARY KEY,
            ticket_id VARCHAR(255) NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'pending',
            payment_method VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);
    log:printInfo("Payment database initialized");
}

service /payment on new http:Listener(9093) {

    resource function get health() returns string {
        return "Payment Service is running";
    }

    resource function get payments/ticket/[string ticketId]() 
            returns Payment|http:NotFound|http:InternalServerError|error {
        
        stream<Payment, sql:Error?> paymentStream = dbClient->query(
            SELECT * FROM payments WHERE ticket_id = ${ticketId}
        );
        
        Payment[] payments = check from Payment payment in paymentStream select payment;
        
        if payments.length() == 0 {
            return http:NOT_FOUND;
        }

        return payments[0];
    }

    resource function get payments() returns Payment[]|http:InternalServerError|error {
        stream<Payment, sql:Error?> paymentStream = dbClient->query(
            SELECT * FROM payments ORDER BY created_at DESC LIMIT 100
        );
        
        Payment[] payments = check from Payment payment in paymentStream select payment;
        return payments;
    }

    resource function get users/[string userId]/payments() 
            returns Payment[]|http:InternalServerError|error {
        
        stream<Payment, sql:Error?> paymentStream = dbClient->query(
            SELECT * FROM payments WHERE user_id = ${userId} ORDER BY created_at DESC
        );
        
        Payment[] payments = check from Payment payment in paymentStream select payment;
        return payments;
    }
}

service on kafkaConsumer {
    remote function onConsumerRecord(kafka:ConsumerRecord[] records) returns error? {
        
        foreach kafka:ConsumerRecord kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check string:fromBytes(value);
            json ticketRequestJson = check message.fromJsonString();

            string ticketId = check ticketRequestJson.ticket_id;
            string userId = check ticketRequestJson.user_id;
            decimal price = check ticketRequestJson.price;

            log:printInfo("Processing payment for ticket: " + ticketId);

            string paymentId = uuid:createType1AsString();
            
            _ = check dbClient->execute(`
                INSERT INTO payments (id, ticket_id, user_id, amount, status, payment_method)
                VALUES (${paymentId}, ${ticketId}, ${userId}, ${price}, 'processing', 'credit_card')
            `);

            int randomNum = check random:createIntInRange(1, 11);
            boolean paymentSuccess = randomNum <= 9;

            if paymentSuccess {
                _ = check dbClient->execute(`
                    UPDATE payments SET status = 'completed' WHERE id = ${paymentId}
                `);

                json paymentProcessed = {
                    "payment_id": paymentId,
                    "ticket_id": ticketId,
                    "user_id": userId,
                    "amount": price,
                    "status": "completed",
                    "timestamp": time:utcNow()
                };

                check kafkaProducer->send({
                    topic: "payments-processed",
                    value: paymentProcessed.toJsonString().toBytes()
                });

                log:printInfo("Payment successful for ticket: " + ticketId);

            } else {
                _ = check dbClient->execute(`
                    UPDATE payments SET status = 'failed' WHERE id = ${paymentId}
                `);

                json paymentFailed = {
                    "payment_id": paymentId,
                    "ticket_id": ticketId,
                    "user_id": userId,
                    "amount": price,
                    "status": "failed",
                    "reason": "Payment gateway declined",
                    "timestamp": time:utcNow()
                };

                check kafkaProducer->send({
                    topic: "payments-failed",
                    value: paymentFailed.toJsonString().toBytes()
                });

                log:printWarn("Payment failed for ticket: " + ticketId);
            }
        }
    }
}

public function main() returns error? {
    check initDatabase();
    log:printInfo("Payment Service started on port 9093");
}
