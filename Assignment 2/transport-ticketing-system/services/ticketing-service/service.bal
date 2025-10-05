import ballerina/http;
import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;
import ballerina/log;
import ballerinax/kafka;
import ballerina/uuid;
import ballerina/time;

configurable string dbHost = "ticketing-db";
configurable int dbPort = 5432;
configurable string dbUser = "ticketing_user";
configurable string dbPassword = "ticketing_pass";
configurable string dbName = "ticketing_db";
configurable string kafkaBootstrapServers = "kafka:29092";

final postgresql:Client dbClient = check new (
    host = dbHost,
    port = dbPort,
    username = dbUser,
    password = dbPassword,
    database = dbName
);

kafka:ProducerConfiguration producerConfig = {
    clientId: "ticketing-service-producer",
    acks: "all",
    retryCount: 3
};

final kafka:Producer kafkaProducer = check new (kafkaBootstrapServers, producerConfig);

kafka:ConsumerConfiguration consumerConfig = {
    groupId: "ticketing-service-group",
    topics: ["payments-processed", "payments-failed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: true
};

listener kafka:Listener kafkaConsumer = new (kafkaBootstrapServers, consumerConfig);

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

service /ticketing on new http:Listener(9092) {

    resource function get health() returns string {
        return "Ticketing Service is running";
    }

    resource function post purchase(@http:Payload PurchaseTicketRequest purchaseReq) 
            returns json|http:InternalServerError|error {
        
        string ticketId = uuid:createType1AsString();
        string currentTime = time:utcToString(time:utcNow());
        
        _ = check dbClient->execute(`
            INSERT INTO tickets (id, user_id, trip_id, ticket_type, status, price, purchase_date)
            VALUES (${ticketId}, ${purchaseReq.user_id}, ${purchaseReq.trip_id}, 
                    ${purchaseReq.ticket_type}, 'CREATED', ${purchaseReq.price}, ${currentTime})
        `);

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

    resource function get users/[string userId]/tickets() 
            returns Ticket[]|http:InternalServerError|error {
        
        stream<Ticket, sql:Error?> ticketStream = dbClient->query(
            `SELECT * FROM tickets WHERE user_id = ${userId} ORDER BY created_at DESC`
        );
        
        Ticket[] tickets = check from Ticket ticket in ticketStream select ticket;
        return tickets;
    }

    resource function post validate(@http:Payload ValidateTicketRequest validateReq) 
            returns json|http:NotFound|http:BadRequest|http:InternalServerError|error {
        
        stream<Ticket, sql:Error?> ticketStream = dbClient->query(
            `SELECT * FROM tickets WHERE id = ${validateReq.ticket_id}`
        );
        
        Ticket[] tickets = check from Ticket ticket in ticketStream select ticket;
        
        if tickets.length() == 0 {
            return http:NOT_FOUND;
        }

        Ticket ticket = tickets[0];

        if ticket.status != "PAID" {
            return <http:BadRequest>{
                body: {
                    "error": "Ticket must be in PAID status. Current: " + ticket.status
                }
            };
        }

        string validationTime = time:utcToString(time:utcNow());
        
        _ = check dbClient->execute(`
            UPDATE tickets 
            SET status = 'VALIDATED', validation_date = ${validationTime}
            WHERE id = ${validateReq.ticket_id}
        `);

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

    resource function get tickets() returns Ticket[]|http:InternalServerError|error {
        stream<Ticket, sql:Error?> ticketStream = dbClient->query(
            `SELECT * FROM tickets ORDER BY created_at DESC LIMIT 100`
        );
        
        Ticket[] tickets = check from Ticket ticket in ticketStream select ticket;
        return tickets;
    }
}

service on kafkaConsumer {
    remote function onConsumerRecord(kafka:ConsumerRecord[] records) returns error? {
        
        foreach kafka:ConsumerRecord kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check string:fromBytes(value);
            json paymentEvent = check message.fromJsonString();

            string topic = kafkaRecord.topic;
            
            if topic == "payments-processed" {
                string ticketId = check paymentEvent.ticket_id;
                
                _ = check dbClient->execute(`
                    UPDATE tickets SET status = 'PAID' WHERE id = ${ticketId}
                `);

                log:printInfo("Ticket marked as PAID: " + ticketId);
                
            } else if topic == "payments-failed" {
                string ticketId = check paymentEvent.ticket_id;
                
                _ = check dbClient->execute(`
                    UPDATE tickets SET status = 'PAYMENT_FAILED' WHERE id = ${ticketId}
                `);

                log:printWarn("Ticket payment failed: " + ticketId);
            }
        }
    }
}

public function main() returns error? {
    check initDatabase();
    log:printInfo("Ticketing Service started on port 9092");
}
