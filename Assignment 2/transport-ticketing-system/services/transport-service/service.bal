import ballerina/http;
import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;
import ballerina/log;
import ballerinax/kafka;
import ballerina/uuid;
import ballerina/time;

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
    clientId: "transport-service-producer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = check new (kafkaBootstrapServers, producerConfig);

// Types
type Route record {|
    string id?;
    string name;
    string transport_type;
    string stops;
    string created_at?;
|};

type Trip record {|
    string id?;
    string route_id;
    string departure_time;
    string arrival_time;
    string status;
    int available_seats;
    string created_at?;
|};

type CreateRouteRequest record {|
    string name;
    string transport_type;
    string stops;
|};

type CreateTripRequest record {|
    string route_id;
    string departure_time;
    string arrival_time;
    int available_seats;
|};

type UpdateTripStatusRequest record {|
    string status;
|};

// Initialize database
function initDatabase() returns error? {
    _ = check dbClient->execute(`
        CREATE TABLE IF NOT EXISTS routes (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            transport_type VARCHAR(50) NOT NULL,
            stops TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);

    _ = check dbClient->execute(`
        CREATE TABLE IF NOT EXISTS trips (
            id VARCHAR(255) PRIMARY KEY,
            route_id VARCHAR(255) NOT NULL,
            departure_time VARCHAR(255) NOT NULL,
            arrival_time VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'scheduled',
            available_seats INT DEFAULT 50,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);

    log:printInfo("Transport database initialized");
}

// HTTP Service
service /transport on new http:Listener(9091) {

    // Health check
    resource function get health() returns string {
        return "Transport Service is running";
    }

    // Create new route
    resource function post routes(@http:Payload CreateRouteRequest routeReq) 
            returns http:Created|http:InternalServerError|error {
        
        string routeId = uuid:createType1AsString();
        
        sql:ExecutionResult result = check dbClient->execute(`
            INSERT INTO routes (id, name, transport_type, stops)
            VALUES (${routeId}, ${routeReq.name}, ${routeReq.transport_type}, ${routeReq.stops})
        `);

        log:printInfo("Route created: " + routeReq.name);
        
        return http:CREATED;
    }

    // Get all routes
    resource function get routes() returns Route[]|http:InternalServerError|error {
        stream<Route, sql:Error?> routeStream = dbClient->query(SELECT * FROM routes);
        Route[] routes = check from Route route in routeStream select route;
        return routes;
    }

    // Get route by ID
    resource function get routes/[string routeId]() 
            returns Route|http:NotFound|http:InternalServerError|error {
        
        stream<Route, sql:Error?> routeStream = dbClient->query(
            SELECT * FROM routes WHERE id = ${routeId}
        );
        
        Route[] routes = check from Route route in routeStream select route;
        
        if routes.length() == 0 {
            return http:NOT_FOUND;
        }

        return routes[0];
    }

    // Create new trip
    resource function post trips(@http:Payload CreateTripRequest tripReq) 
            returns http:Created|http:InternalServerError|error {
        
        string tripId = uuid:createType1AsString();
        
        sql:ExecutionResult result = check dbClient->execute(`
            INSERT INTO trips (id, route_id, departure_time, arrival_time, available_seats, status)
            VALUES (${tripId}, ${tripReq.route_id}, ${tripReq.departure_time}, 
                    ${tripReq.arrival_time}, ${tripReq.available_seats}, 'scheduled')
        `);

        // Publish schedule update event
        json scheduleEvent = {
            "event_type": "trip_created",
            "trip_id": tripId,
            "route_id": tripReq.route_id,
            "departure_time": tripReq.departure_time,
            "arrival_time": tripReq.arrival_time,
            "available_seats": tripReq.available_seats,
            "timestamp": time:utcNow()
        };

        check kafkaProducer->send({
            topic: "schedule-updates",
            value: scheduleEvent.toJsonString().toBytes()
        });

        log:printInfo("Trip created for route: " + tripReq.route_id);
        
        return http:CREATED;
    }

    // Get all trips
    resource function get trips() returns Trip[]|http:InternalServerError|error {
        stream<Trip, sql:Error?> tripStream = dbClient->query(SELECT * FROM trips ORDER BY departure_time);
        Trip[] trips = check from Trip trip in tripStream select trip;
        return trips;
    }

    // Get trip by ID
    resource function get trips/[string tripId]() 
            returns Trip|http:NotFound|http:InternalServerError|error {
        
        stream<Trip, sql:Error?> tripStream = dbClient->query(
            SELECT * FROM trips WHERE id = ${tripId}
        );
        
        Trip[] trips = check from Trip trip in tripStream select trip;
        
        if trips.length() == 0 {
            return http:NOT_FOUND;
        }

        return trips[0];
    }

    // Update trip status
    resource function put trips/[string tripId]/status(@http:Payload UpdateTripStatusRequest statusReq) 
            returns http:Ok|http:NotFound|http:InternalServerError|error {
        
        sql:ExecutionResult result = check dbClient->execute(`
            UPDATE trips SET status = ${statusReq.status} WHERE id = ${tripId}
        `);

        if result.affectedRowCount == 0 {
            return http:NOT_FOUND;
        }

        // Publish schedule update event
        json updateEvent = {
            "event_type": "trip_status_updated",
            "trip_id": tripId,
            "new_status": statusReq.status,
            "timestamp": time:utcNow()
        };

        check kafkaProducer->send({
            topic: "schedule-updates",
            value: updateEvent.toJsonString().toBytes()
        });

        log:printInfo("Trip status updated: " + tripId + " -> " + statusReq.status);
        
        return http:OK;
    }

    // Get trips by route
    resource function get routes/[string routeId]/trips() 
            returns Trip[]|http:InternalServerError|error {
        
        stream<Trip, sql:Error?> tripStream = dbClient->query(
            SELECT * FROM trips WHERE route_id = ${routeId} ORDER BY departure_time
        );
        
        Trip[] trips = check from Trip trip in tripStream select trip;
        return trips;
    }
}

// Main function
public function main() returns error? {
    check initDatabase();
    log:printInfo("Transport Service started on port 9091");
}
