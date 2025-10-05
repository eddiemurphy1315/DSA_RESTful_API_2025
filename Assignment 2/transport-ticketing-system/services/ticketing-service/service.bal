//Mbanga
import ballerina/http;
import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;
import ballerina/log;
import ballerinax/kafka;
import ballerina/uuid;
import ballerina/time;
import ballerina/lang.runtime;

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
    clientId: "ticketing-service-producer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = check new (kafkaBootstrapServers, producerConfig);

// Kafka consumer for payment confirmations
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "ticketing-service-group",
    topics: ["payments-processed", "payments-failed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: true
};

listener kafka:Listener kafkaConsumer = new (kafkaBootstrapServers, consumerConfig);

// Types
type Ticket record {|
    string id?;
    string user_id;
    string trip_id;
    string ticket_type;
    string status;
    decimal price;
    string purchase_date?;
    string validation_date?;
    string created_at?;
|};

type PurchaseTicketRequest record {|
    string user_id;
    string trip_id;
    string ticket_type;
    decimal price;
|};

type ValidateTicketRequest record {|
    string ticket_id;
|};

type PaymentEvent record {
    string payment_id;
    string ticket_id;
    string status;
    decimal amount;
};

// Initialize database
function initDatabase() returns error? {
    _ = check dbClient->execute(`
        CREATE TABLE IF NOT EXISTS tickets (
            id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            trip_id VARCHAR(255) NOT NULL,
            ticket_type VARCHAR(50) NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'CREATED',
            price DECIMAL(10,2) NOT NULL,
            purchase_date VARCHAR(255),
            validation_date VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);

    log:printInfo("Ticketing database initialized");
}

// HTTP Service
service /ticketing on new http:Listener(9092) {

    // Health check
    resource function get health() returns string {
        return "Ticketing Service is running";
    }

    // Purchase ticket
    resource function post purchase(@http:Payload PurchaseTicketRequest purchaseReq) 
            returns json|http:InternalServerError|error {
        
        string ticketId = uuid:createType1AsString();
        string currentTime = time:utcToString(time:utcNow());
        
        // Create ticket with CREATED status
        sql:ExecutionResult result = check dbClient->execute(`
            INSERT INTO tickets (id, user_id, trip_id, ticket_type, status, price, purchase_date)
            VALUES (${ticketId}, ${purchaseReq.user_id}, ${purchaseReq.trip_id}, 
                    ${purchaseReq.ticket_type}, 'CREATED', ${purchaseReq.price}, ${currentTime})
        `);

        // Publish ticket request to Kafka for payment processing
        json ticketRequest = {
            "ticket_id": ticketId,
            "user_id": purchaseReq.user_id,
            "trip_id": purchaseReq.trip_id,
            "ticket_type": purchaseReq.ticket_type,
            "price": purchaseReq.price,
            "timestamp": time:utcNow()
        };

        check kafkaProducer->send({
            topic: "ticket-requests",
            value: ticketRequest.toJsonString().toBytes()
        });

        // Also publish ticket-created event
        json ticketCreated = {
            "event_type": "ticket_created",
            "ticket_id": ticketId,
            "user_id": purchaseReq.user_id,
            "trip_id": purchaseReq.trip_id,
            "status": "CREATED",
            "timestamp": time:utcNow()
        };

        check kafkaProducer->send({
            topic: "ticket-created",
            value: ticketCreated.toJsonString().toBytes()
        });

        log:printInfo("Ticket purchase initiated: " + ticketId);
        
        return {
            "ticket_id": ticketId,
            "status": "CREATED",
            "message": "Ticket created, awaiting payment"
        };
    }

    // Get ticket by ID
    resource function get tickets/[string ticketId]() 
            returns Ticket|http:NotFound|http:InternalServerError|error {
        
        stream<Ticket, sql:Error?> ticketStream = dbClient->query(
            `SELECT * FROM tickets WHERE id = ${ticketId}`
        );
        
        Ticket[] tickets = check from Ticket ticket in ticketStream select ticket;
        
        if tickets.length() == 0 {
            return http:NOT_FOUND;
        }

        return tickets[0];
    }

    // Get user's tickets
    resource function get users/[string userId]/tickets() 
            returns Ticket[]|http:InternalServerError|error {
        
        stream<Ticket, sql:Error?> ticketStream = dbClient->query(
            `SELECT * FROM tickets WHERE user_id = ${userId} ORDER BY created_at DESC`
        );
        
        Ticket[] tickets = check from Ticket ticket in ticketStream select ticket;
        return tickets;
    }

    // Validate ticket
    resource function post validate(@http:Payload ValidateTicketRequest validateReq) 
            returns json|http:NotFound|http:BadRequest|http:InternalServerError|error {
        
        // Get ticket
        stream<Ticket, sql:Error?> ticketStream = dbClient->query(
            `SELECT * FROM tickets WHERE id = ${validateReq.ticket_id}`
        );
        
        Ticket[] tickets = check from Ticket ticket in ticketStream select ticket;
        
        if tickets.length() == 0 {
            return http:NOT_FOUND;
        }

        Ticket ticket = tickets[0];

        // Check if ticket is PAID
        if ticket.status != "PAID" {
            return <http:BadRequest>{
                body: {
                    "error": "Ticket must be in PAID status to validate. Current status: " + ticket.status
                }
            };
        }

        // Update ticket to VALIDATED
        string validationTime = time:utcToString(time:utcNow());
        
        sql:ExecutionResult result = check dbClient->execute(`
            UPDATE tickets 
            SET status = 'VALIDATED', validation_date = ${validationTime}
            WHERE id = ${validateReq.ticket_id}
        `);

        // Publish validation event
        json validationEvent = {
            "event_type": "ticket_validated",
            "ticket_id": validateReq.ticket_id,
            "user_id": ticket.user_id,
            "trip_id": ticket.trip_id,
            "validation_time": validationTime,
            "timestamp": time:utcNow()
        };

        check kafkaProducer->send({
            topic: "ticket-validated",
            value: validationEvent.toJsonString().toBytes()
        });

        log:printInfo("Ticket validated: " + validateReq.ticket_id);

        return {
            "ticket_id": validateReq.ticket_id,
            "status": "VALIDATED",
            "validation_time": validationTime,
            "message": "Ticket successfully validated"
        };
    }

    // Get all tickets (for admin)
    resource function get tickets() returns Ticket[]|http:InternalServerError|error {
        stream<Ticket, sql:Error?> ticketStream = dbClient->query(
            `SELECT * FROM tickets ORDER BY created_at DESC LIMIT 100`
        );
        
        Ticket[] tickets = check from Ticket ticket in ticketStream select ticket;
        return tickets;
    }
}

// Kafka consumer service for payment events
service on kafkaConsumer {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        
        foreach kafka:ConsumerRecord kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check string:fromBytes(value);
            json paymentEvent = check message.fromJsonString();

            string topic = kafkaRecord.topic;
            
            if topic == "payments-processed" {
                // Payment successful - update ticket to PAID
                string ticketId = check paymentEvent.ticket_id;
                
                sql:ExecutionResult result = check dbClient->execute(`
                    UPDATE tickets SET status = 'PAID' WHERE id = ${ticketId}
                `);

                log:printInfo("Ticket marked as PAID: " + ticketId);
                
            } else if topic == "payments-failed" {
                // Payment failed - ticket remains in CREATED status or mark as FAILED
                string ticketId = check paymentEvent.ticket_id;
                
                sql:ExecutionResult result = check dbClient->execute(`
                    UPDATE tickets SET status = 'PAYMENT_FAILED' WHERE id = ${ticketId}
                `);

                log:printWarn("Ticket payment failed: " + ticketId);
            }
        }
    }
}

// Main function
public function main() returns error? {
    check initDatabase();
    log:printInfo("Ticketing Service started on port 9092");
    log:printInfo("Kafka consumer started for payment events");
    
    // Keep the service running
    runtime:sleep(3600);
}
