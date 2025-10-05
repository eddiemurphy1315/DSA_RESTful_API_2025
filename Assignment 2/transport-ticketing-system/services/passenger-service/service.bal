//Henry
import ballerina/http;
import ballerina/uuid;
import ballerinax/mongodb;
import ballerina/time;
import ballerina/log;

// MongoDB configuration
configurable string mongoHost = "mongodb";
configurable int mongoPort = 27017;
configurable string mongoDatabase = "ticketing_system";

// MongoDB client
final mongodb:Client mongoClient = check new ({
    connection: {
        serverAddress: {
            host: mongoHost,
            port: mongoPort
        }
    }
});

// Passenger type definition
type Passenger record {|
    string _id?;
    string passengerId;
    string firstName;
    string lastName;
    string email;
    string phoneNumber;
    string passwordHash;
    string accountStatus; // ACTIVE, SUSPENDED, CLOSED
    time:Utc createdAt;
    time:Utc updatedAt;
|};

// Request/Response types
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

type ErrorResponse record {|
    string message;
    string ei_code?;
|};

// Simple password hashing (in production, use bcrypt or similar)
function hashPassword(string password) returns string {
    // For demo purposes - in production use proper hashing library
    return "hashed_" + password;
}

function verifyPassword(string password, string hash) returns boolean {
    return hashPassword(password) == hash;
}

// Generate simple JWT-like token (in production, use proper JWT library)
function generateToken(string passengerId) returns string {
    return "token_" + passengerId + "_" + time:utcNow()[0].toString();
}

// Service listener
listener http:Listener passengerListener = new (8081);

// Passenger Service
service /api/passengers on passengerListener {

    // Health check endpoint
    resource function get health() returns json {
        return {
            status: "UP",
            serviceName: "Passenger Service",
            timestamp: time:utcToString(time:utcNow())
        };
    }

    // Register new passenger
    resource function post register(RegisterRequest request) returns http:Created|http:Conflict|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection passengers = check db->getCollection("passengers");

            // Check if email already exists
            map<json> filter = {"email": request.email};
            stream<Passenger, error?> existingStream = check passengers->find(filter);
            Passenger[] existing = check from Passenger p in existingStream select p;
            
            if existing.length() > 0 {
                return <http:Conflict>{
                    body: {
                        message: "Email already registered"
                    }
                };
            }

            // Create new passenger
            string passengerId = uuid:createType1AsString();
            time:Utc now = time:utcNow();
            
            Passenger newPassenger = {
                passengerId: passengerId,
                firstName: request.firstName,
                lastName: request.lastName,
                email: request.email,
                phoneNumber: request.phoneNumber,
                passwordHash: hashPassword(request.password),
                accountStatus: "ACTIVE",
                createdAt: now,
                updatedAt: now
            };

            check passengers->insertOne(newPassenger);

            log:printInfo(string `New passenger registered: ${passengerId}`);

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
                    ei_code: e.message()
                }
            };
        }
    }

    // Login passenger
    resource function post login(LoginRequest request) returns LoginResponse|http:Unauthorized|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection passengers = check db->getCollection("passengers");

            // Find passenger by email
            map<json> filter = {"email": request.email};
            stream<Passenger, error?> passengerStream = check passengers->find(filter);
            Passenger[] results = check from Passenger p in passengerStream select p;

            if results.length() == 0 {
                return <http:Unauthorized>{
                    body: {
                        message: "Invalid credentials"
                    }
                };
            }

            Passenger passenger = results[0];

            // Verify password
            if !verifyPassword(request.password, passenger.passwordHash) {
                return <http:Unauthorized>{
                    body: {
                        message: "Invalid credentials"
                    }
                };
            }

            // Check account status
            if passenger.accountStatus != "ACTIVE" {
                return <http:Unauthorized>{
                    body: {
                        message: "Account is not active"
                    }
                };
            }

            // Generate token
            string token = generateToken(passenger.passengerId);

            log:printInfo(string `Passenger logged in: ${passenger.passengerId}`);

            return {
                passengerId: passenger.passengerId,
                email: passenger.email,
                firstName: passenger.firstName,
                lastName: passenger.lastName,
                token: token,
                message: "Login successful"
            };

        } on fail var e {
            log:printError("Login failed", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Login failed",
                    ei_code: e.message()
                }
            };
        }
    }

    // Get passenger profile
    resource function get [string passengerId]() returns PassengerResponse|http:NotFound|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection passengers = check db->getCollection("passengers");

            map<json> filter = {"passengerId": passengerId};
            stream<Passenger, error?> passengerStream = check passengers->find(filter);
            Passenger[] results = check from Passenger p in passengerStream select p;

            if results.length() == 0 {
                return <http:NotFound>{
                    body: {
                        message: "Passenger not found"
                    }
                };
            }

            Passenger passenger = results[0];

            return {
                passengerId: passenger.passengerId,
                firstName: passenger.firstName,
                lastName: passenger.lastName,
                email: passenger.email,
                phoneNumber: passenger.phoneNumber,
                accountStatus: passenger.accountStatus,
                createdAt: time:utcToString(passenger.createdAt)
            };

        } on fail var e {
            log:printError("Failed to get passenger", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve passenger",
                    ei_code: e.message()
                }
            };
        }
    }

    // Update passenger account
    resource function put [string passengerId](UpdateAccountRequest request) returns PassengerResponse|http:NotFound|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection passengers = check db->getCollection("passengers");

            // Check if passenger exists
            map<json> filter = {"passengerId": passengerId};
            stream<Passenger, error?> passengerStream = check passengers->find(filter);
            Passenger[] results = check from Passenger p in passengerStream select p;

            if results.length() == 0 {
                return <http:NotFound>{
                    body: {
                        message: "Passenger not found"
                    }
                };
            }

            // Build update document
            map<json> updateDoc = {
                "updatedAt": time:utcNow()
            };

            if request.firstName is string {
                updateDoc["firstName"] = request.firstName;
            }
            if request.lastName is string {
                updateDoc["lastName"] = request.lastName;
            }
            if request.phoneNumber is string {
                updateDoc["phoneNumber"] = request.phoneNumber;
            }

            mongodb:Update update = { "$set": updateDoc };
            mongodb:UpdateResult updateResult = check passengers->updateOne(filter, update);

            log:printInfo(string `Passenger updated: ${passengerId}`);

            // Fetch updated passenger
            stream<Passenger, error?> updatedStream = check passengers->find(filter);
            Passenger[] updatedResults = check from Passenger p in updatedStream select p;
            Passenger updated = updatedResults[0];

            return {
                passengerId: updated.passengerId,
                firstName: updated.firstName,
                lastName: updated.lastName,
                email: updated.email,
                phoneNumber: updated.phoneNumber,
                accountStatus: updated.accountStatus,
                createdAt: time:utcToString(updated.createdAt)
            };

        } on fail var e {
            log:printError("Failed to update passenger", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to update passenger",
                    ei_code: e.message()
                }
            };
        }
    }

    // Get passenger's tickets
    resource function get [string passengerId]/tickets() returns TicketResponse[]|http:NotFound|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection tickets = check db->getCollection("tickets");

            // Find all tickets for this passenger
            map<json> filter = {"passengerId": passengerId};
            stream<record {|string ticketId; string passengerId; string ticketType; string status; decimal price; time:Utc purchasedAt; time:Utc? validatedAt; time:Utc? expiresAt;|}, error?> ticketStream = check tickets->find(filter);
            
            TicketResponse[] ticketResponses = [];
            
            check from var ticket in ticketStream
                do {
                    ticketResponses.push({
                        ticketId: ticket.ticketId,
                        passengerId: ticket.passengerId,
                        ticketType: ticket.ticketType,
                        status: ticket.status,
                        price: ticket.price,
                        purchasedAt: time:utcToString(ticket.purchasedAt),
                        validatedAt: ticket.validatedAt is time:Utc ? time:utcToString(<time:Utc>ticket.validatedAt) : (),
                        expiresAt: ticket.expiresAt is time:Utc ? time:utcToString(<time:Utc>ticket.expiresAt) : ()
                    });
                };

            log:printInfo(string `Retrieved ${ticketResponses.length()} tickets for passenger: ${passengerId}`);

            return ticketResponses;

        } on fail var e {
            log:printError("Failed to get tickets", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve tickets",
                    ei_code: e.message()
                }
            };
        }
    }

    // Delete/Close passenger account
    resource function delete [string passengerId]() returns http:Ok|http:NotFound|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection passengers = check db->getCollection("passengers");

            // Update status to CLOSED instead of deleting
            map<json> filter = {"passengerId": passengerId};
            mongodb:Update update = {
                "$set": {
                    "accountStatus": "CLOSED",
                    "updatedAt": time:utcNow()
                }
            };

            mongodb:UpdateResult result = check passengers->updateOne(filter, update);

            if result.modifiedCount == 0 {
                return <http:NotFound>{
                    body: {
                        message: "Passenger not found"
                    }
                };
            }

            log:printInfo(string `Passenger account closed: ${passengerId}`);

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
                    ei_code: e.message()
                }
            };
        }
    }
}