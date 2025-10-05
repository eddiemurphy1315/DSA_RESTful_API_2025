//henry boss//
import ballerina/http;
import ballerina/uuid;
import ballerina/sql;
import ballerinax/postgresql;
import ballerina/time;
import ballerina/log;
import ballerinax/kafka;

// Database configuration
configurable string dbHost = "passenger-db";
configurable int dbPort = 5432;
configurable string dbUser = "passenger_user";
configurable string dbPassword = "passenger_pass";
configurable string dbName = "passenger_db";

// Kafka configuration
configurable string kafkaBootstrapServers = "kafka:29092";

// PostgreSQL client
final postgresql:Client dbClient = check new (
    host = dbHost,
    port = dbPort,
    username = dbUser,
    password = dbPassword,
    database = dbName
);

// Kafka producer for events
final kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, {
    clientId: "passenger-service-producer",
    acks: "all",
    retryCount: 3
});

// Type definitions
type Passenger record {|
    string passenger_id;
    string first_name;
    string last_name;
    string email;
    string phone_number;
    string password_hash;
    string account_status;
    time:Civil created_at;
    time:Civil updated_at;
|};

type RegisterRequest record {|
    string firstName;
    string lastName;
    string email;
    string phoneNumber;
    string password;
|};

type LoginRequest record {|
    string email;
    string password;
|};

type UpdateAccountRequest record {|
    string? firstName;
    string? lastName;
    string? phoneNumber;
|};

type LoginResponse record {|
    string passengerId;
    string email;
    string firstName;
    string lastName;
    string token;
    string message;
|};

type PassengerResponse record {|
    string passengerId;
    string firstName;
    string lastName;
    string email;
    string phoneNumber;
    string accountStatus;
    string createdAt;
|};

type TicketResponse record {|
    string ticketId;
    string passengerId;
    string ticketType;
    string status;
    decimal price;
    string purchasedAt;
    string? validatedAt;
    string? expiresAt;
|};

type PassengerRegisteredEvent record {|
    string eventType;
    string passengerId;
    string email;
    string firstName;
    string lastName;
    string timestamp;
|};

// Initialize database schema
function initDatabase() returns error? {
    _ = check dbClient->execute(`
        CREATE TABLE IF NOT EXISTS passengers (
            id SERIAL PRIMARY KEY,
            passenger_id VARCHAR(255) UNIQUE NOT NULL,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            phone_number VARCHAR(20) NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            account_status VARCHAR(20) DEFAULT 'ACTIVE',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);
    
    _ = check dbClient->execute(`
        CREATE INDEX IF NOT EXISTS idx_passenger_email ON passengers(email)
    `);
    
    _ = check dbClient->execute(`
        CREATE INDEX IF NOT EXISTS idx_passenger_id ON passengers(passenger_id)
    `);
    
    log:printInfo("Database schema initialized successfully");
}

// Password hashing (simple version - use bcrypt in production)
function hashPassword(string password) returns string {
    return "hashed_" + password;
}

function verifyPassword(string password, string hash) returns boolean {
    return hashPassword(password) == hash;
}

// Generate token
function generateToken(string passengerId) returns string {
    return "token_" + passengerId + "_" + time:utcNow()[0].toString();
}

// Publish event to Kafka
function publishEvent(string topic, json event) {
    do {
        check kafkaProducer->send({
            topic: topic,
            value: event.toJsonString().toBytes()
        });
        log:printInfo(string `Event published to ${topic}: ${event.toJsonString()}`);
    } on fail var e {
        log:printError("Failed to publish event to Kafka", 'error = e);
    }
}

// Service initialization
function init() returns error? {
    check initDatabase();
    log:printInfo("Passenger Service initialized successfully");
}

// Service listener
listener http:Listener passengerListener = new (9090);

// Passenger Service
service /api/passengers on passengerListener {

    // Health check
    resource function get health() returns json {
        return {
            status: "UP",
            serviceName: "Passenger Service",
            timestamp: time:utcToString(time:utcNow()),
            database: "PostgreSQL",
            port: 9090
        };
    }

    // Register new passenger
    resource function post register(RegisterRequest request) returns http:Created|http:Conflict|http:InternalServerError {
        do {
            // Check if email already exists
            Passenger|sql:Error existingPassenger = dbClient->queryRow(
                `SELECT * FROM passengers WHERE email = ${request.email}`
            );

            if existingPassenger is Passenger {
                return <http:Conflict>{
                    body: {
                        message: "Email already registered"
                    }
                };
            }

            // Create new passenger
            string passengerId = uuid:createType1AsString();
            string passwordHash = hashPassword(request.password);
            time:Civil now = time:utcToCivil(time:utcNow());

            _ = check dbClient->execute(`
                INSERT INTO passengers (
                    passenger_id, first_name, last_name, email, 
                    phone_number, password_hash, account_status, 
                    created_at, updated_at
                ) VALUES (
                    ${passengerId}, ${request.firstName}, ${request.lastName}, 
                    ${request.email}, ${request.phoneNumber}, ${passwordHash}, 
                    'ACTIVE', ${now}, ${now}
                )
            `);

            log:printInfo(string `New passenger registered: ${passengerId}`);

            // Publish event to Kafka
            PassengerRegisteredEvent event = {
                eventType: "PASSENGER_REGISTERED",
                passengerId: passengerId,
                email: request.email,
                firstName: request.firstName,
                lastName: request.lastName,
                timestamp: time:utcToString(time:utcNow())
            };
            publishEvent("passenger.events", event.toJson());

            return <http:Created>{
                headers: {
                    "Location": string `/api/passengers/${passengerId}`
                },
                body: {
                    message: "Passenger registered successfully",
                    passengerId: passengerId,
                    email: request.email
                }
            };

        } on fail var e {
            log:printError("Registration failed", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Registration failed",
                    error: e.message()
                }
            };
        }
    }

    // Login passenger
    resource function post login(LoginRequest request) returns LoginResponse|http:Unauthorized|http:InternalServerError {
        do {
            Passenger passenger = check dbClient->queryRow(
                `SELECT * FROM passengers WHERE email = ${request.email}`
            );

            // Verify password
            if !verifyPassword(request.password, passenger.password_hash) {
                return <http:Unauthorized>{
                    body: {
                        message: "Invalid credentials"
                    }
                };
            }

            // Check account status
            if passenger.account_status != "ACTIVE" {
                return <http:Unauthorized>{
                    body: {
                        message: "Account is not active"
                    }
                };
            }

            string token = generateToken(passenger.passenger_id);
            log:printInfo(string `Passenger logged in: ${passenger.passenger_id}`);

            // Publish login event
            json loginEvent = {
                eventType: "PASSENGER_LOGIN",
                passengerId: passenger.passenger_id,
                timestamp: time:utcToString(time:utcNow())
            };
            publishEvent("passenger.events", loginEvent);

            return {
                passengerId: passenger.passenger_id,
                email: passenger.email,
                firstName: passenger.first_name,
                lastName: passenger.last_name,
                token: token,
                message: "Login successful"
            };

        } on fail var e {
            log:printError("Login failed", 'error = e);
            if e is sql:NoRowsError {
                return <http:Unauthorized>{
                    body: {
                        message: "Invalid credentials"
                    }
                };
            }
            return <http:InternalServerError>{
                body: {
                    message: "Login failed",
                    error: e.message()
                }
            };
        }
    }

    // Get passenger profile
    resource function get [string passengerId]() returns PassengerResponse|http:NotFound|http:InternalServerError {
        do {
            Passenger passenger = check dbClient->queryRow(
                `SELECT * FROM passengers WHERE passenger_id = ${passengerId}`
            );

            return {
                passengerId: passenger.passenger_id,
                firstName: passenger.first_name,
                lastName: passenger.last_name,
                email: passenger.email,
                phoneNumber: passenger.phone_number,
                accountStatus: passenger.account_status,
                createdAt: passenger.created_at.toString()
            };

        } on fail var e {
            log:printError("Failed to get passenger", 'error = e);
            if e is sql:NoRowsError {
                return <http:NotFound>{
                    body: {
                        message: "Passenger not found"
                    }
                };
            }
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve passenger",
                    error: e.message()
                }
            };
        }
    }

    // Update passenger account
    resource function put [string passengerId](UpdateAccountRequest request) returns PassengerResponse|http:NotFound|http:InternalServerError {
        do {
            // Check if passenger exists
            Passenger existing = check dbClient->queryRow(
                `SELECT * FROM passengers WHERE passenger_id = ${passengerId}`
            );

            // Build update query
            string firstName = request.firstName ?: existing.first_name;
            string lastName = request.lastName ?: existing.last_name;
            string phoneNumber = request.phoneNumber ?: existing.phone_number;
            time:Civil now = time:utcToCivil(time:utcNow());

            _ = check dbClient->execute(`
                UPDATE passengers 
                SET first_name = ${firstName}, 
                    last_name = ${lastName}, 
                    phone_number = ${phoneNumber},
                    updated_at = ${now}
                WHERE passenger_id = ${passengerId}
            `);

            log:printInfo(string `Passenger updated: ${passengerId}`);

            // Fetch updated passenger
            Passenger updated = check dbClient->queryRow(
                `SELECT * FROM passengers WHERE passenger_id = ${passengerId}`
            );

            // Publish update event
            json updateEvent = {
                eventType: "PASSENGER_UPDATED",
                passengerId: passengerId,
                timestamp: time:utcToString(time:utcNow())
            };
            publishEvent("passenger.events", updateEvent);

            return {
                passengerId: updated.passenger_id,
                firstName: updated.first_name,
                lastName: updated.last_name,
                email: updated.email,
                phoneNumber: updated.phone_number,
                accountStatus: updated.account_status,
                createdAt: updated.created_at.toString()
            };

        } on fail var e {
            log:printError("Failed to update passenger", 'error = e);
            if e is sql:NoRowsError {
                return <http:NotFound>{
                    body: {
                        message: "Passenger not found"
                    }
                };
            }
            return <http:InternalServerError>{
                body: {
                    message: "Failed to update passenger",
                    error: e.message()
                }
            };
        }
    }

    // Get passenger's tickets
    resource function get [string passengerId]/tickets() returns TicketResponse[]|http:InternalServerError {
        do {
            stream<record {|
                string ticket_id;
                string passenger_id;
                string ticket_type;
                string status;
                decimal price;
                time:Civil purchased_at;
                time:Civil? validated_at;
                time:Civil? expires_at;
            |}, sql:Error?> ticketStream = dbClient->query(
                `SELECT ticket_id, passenger_id, ticket_type, status, price, 
                        purchased_at, validated_at, expires_at 
                 FROM tickets 
                 WHERE passenger_id = ${passengerId}
                 ORDER BY purchased_at DESC`
            );

            TicketResponse[] tickets = [];
            check from var ticket in ticketStream
                do {
                    tickets.push({
                        ticketId: ticket.ticket_id,
                        passengerId: ticket.passenger_id,
                        ticketType: ticket.ticket_type,
                        status: ticket.status,
                        price: ticket.price,
                        purchasedAt: ticket.purchased_at.toString(),
                        validatedAt: ticket.validated_at is time:Civil ? 
                            (<time:Civil>ticket.validated_at).toString() : (),
                        expiresAt: ticket.expires_at is time:Civil ? 
                            (<time:Civil>ticket.expires_at).toString() : ()
                    });
                };

            log:printInfo(string `Retrieved ${tickets.length()} tickets for passenger: ${passengerId}`);
            return tickets;

        } on fail var e {
            log:printError("Failed to get tickets", 'error = e);
            // Return empty array if tickets table doesn't exist yet or no tickets found
            return [];
        }
    }

    // Close passenger account
    resource function delete [string passengerId]() returns http:Ok|http:NotFound|http:InternalServerError {
        do {
            time:Civil now = time:utcToCivil(time:utcNow());
            
            sql:ExecutionResult result = check dbClient->execute(`
                UPDATE passengers 
                SET account_status = 'CLOSED', updated_at = ${now}
                WHERE passenger_id = ${passengerId}
            `);

            if result.affectedRowCount == 0 {
                return <http:NotFound>{
                    body: {
                        message: "Passenger not found"
                    }
                };
            }

            log:printInfo(string `Passenger account closed: ${passengerId}`);

            // Publish account closure event
            json closureEvent = {
                eventType: "PASSENGER_ACCOUNT_CLOSED",
                passengerId: passengerId,
                timestamp: time:utcToString(time:utcNow())
            };
            publishEvent("passenger.events", closureEvent);

            return <http:Ok>{
                body: {
                    message: "Account closed successfully"
                }
            };

        } on fail var e {
            log:printError("Failed to close account", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to close account",
                    error: e.message()
                }
            };
        }
    }

    // Get all passengers (for admin/debugging)
    resource function get .() returns PassengerResponse[]|http:InternalServerError {
        do {
            stream<Passenger, sql:Error?> passengerStream = dbClient->query(
                `SELECT * FROM passengers ORDER BY created_at DESC LIMIT 100`
            );

            PassengerResponse[] passengers = [];
            check from var passenger in passengerStream
                do {
                    passengers.push({
                        passengerId: passenger.passenger_id,
                        firstName: passenger.first_name,
                        lastName: passenger.last_name,
                        email: passenger.email,
                        phoneNumber: passenger.phone_number,
                        accountStatus: passenger.account_status,
                        createdAt: passenger.created_at.toString()
                    });
                };

            return passengers;

        } on fail var e {
            log:printError("Failed to get passengers", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve passengers",
                    error: e.message()
                }
            };
        }
    }
}