//Excellence
import ballerina/http;
import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;
import ballerina/log;
import ballerinax/kafka;
import ballerina/uuid;
import ballerina/time;
import ballerina/lang.runtime;
import ballerina/random;

// Configuration
configurable string dbHost = ?;
configurable int dbPort = ?;
configurable string dbUser = ?;
configurable string dbPassword = ?;
configurable string dbName = ?;
configurable string kafkaBootstrapServers = ?;

// Database client
postgresql:Client dbClient = check new (
    host = dbHost,
    port = dbPort,
    user = dbUser,
    password = dbPassword,
    database = dbName
);

// Kafka producer
kafka:ProducerConfiguration producerConfig = {
    clientId: "payment-service-producer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = check new (kafkaBootstrapServers, producerConfig);

// Kafka consumer for ticket requests
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "payment-service-group",
    topics: ["ticket-requests"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: true
};

listener kafka:Listener kafkaConsumer = new (kafkaBootstrapServers, consumerConfig);

// Types
type Payment record {|
    string id?;
    string ticket_id;
    string user_id;
    decimal amount;
    string status;
    string payment_method?;
    string created_at?;
|};

type TicketRequest record {
    string ticket_id;
    string user_id;
    string trip_id;
    string ticket_type;
    decimal price;
};

// Initialize database
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

// Simulate payment processing
function processPayment(string ticketId, decimal amount) returns boolean {
    // Simulate payment with 90% success rate
    // In production, this would integrate with real payment gateway
    int randomNum = checkpanic random:createIntInRange(1, 11);
    return randomNum <= 9; // 90% success rate
}

// HTTP Service
service /payment on new http:Listener(9093) {

    // Health check
    resource function get health() returns string {
        return "Payment Service is running";
    }

    // Get payment by ticket ID
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

    // Get all payments (for admin)
    resource function get payments() returns Payment[]|http:InternalServerError|error {
        stream<Payment, sql:Error?> paymentStream = dbClient->query(
            SELECT * FROM payments ORDER BY created_at DESC LIMIT 100
        );
        
        Payment[] payments = check from Payment payment in paymentStream select payment;
        return payments;
    }

    // Get user's payments
    resource function get users/[string userId]/payments() 
            returns Payment[]|http:InternalServerError|error {
        
        stream<Payment, sql:Error?> paymentStream = dbClient->query(
            SELECT * FROM payments WHERE user_id = ${userId} ORDER BY created_at DESC
        );
        
        Payment[] payments = check from Payment payment in paymentStream select payment;
        return payments;
    }
}

// Kafka consumer service for ticket requests
service on kafkaConsumer {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        
        foreach kafka:ConsumerRecord kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check string:fromBytes(value);
            json ticketRequestJson = check message.fromJsonString();

            // Parse ticket request
            string ticketId = check ticketRequestJson.ticket_id;
            string userId = check ticketRequestJson.user_id;
            decimal price = check ticketRequestJson.price;

            log:printInfo("Processing payment for ticket: " + ticketId);

            // Create payment record
            string paymentId = uuid:createType1AsString();
            
            sql:ExecutionResult result = check dbClient->execute(`
                INSERT INTO payments (id, ticket_id, user_id, amount, status, payment_method)
                VALUES (${paymentId}, ${ticketId}, ${userId}, ${price}, 'processing', 'credit_card')
            `);

            // Simulate payment processing delay
            runtime:sleep(2); // 2 seconds delay

            // Process payment (90% success rate)
            boolean paymentSuccess = processPayment(ticketId, price);

            if paymentSuccess {
                // Payment succeeded
                _ = check dbClient->execute(`
                    UPDATE payments SET status = 'completed' WHERE id = ${paymentId}
                `);

                // Publish payment success event
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
                // Payment failed
                _ = check dbClient->execute(`
                    UPDATE payments SET status = 'failed' WHERE id = ${paymentId}
                `);

                // Publish payment failure event
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

// Main function
public function main() returns error? {
    check initDatabase();
    log:printInfo("Payment Service started on port 9093");
    log:printInfo("Kafka consumer started for ticket-requests");
    
    // Keep the service running
    runtime:sleep(3600);
}
