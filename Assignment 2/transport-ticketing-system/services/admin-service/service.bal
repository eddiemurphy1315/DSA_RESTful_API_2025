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
    clientId: "admin-service-producer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = check new (kafkaBootstrapServers, producerConfig);

// HTTP clients to other services
http:Client passengerServiceClient = check new ("http://passenger-service:9090");
http:Client transportServiceClient = check new ("http://transport-service:9091");
http:Client ticketingServiceClient = check new ("http://ticketing-service:9092");
http:Client paymentServiceClient = check new ("http://payment-service:9093");
http:Client notificationServiceClient = check new ("http://notification-service:9094");

// Types
type ServiceDisruption record {|
    string id?;
    string disruption_type;
    string title;
    string message;
    string affected_routes?;
    string severity;
    string created_at?;
|};

type DisruptionRequest record {|
    string disruption_type;
    string title;
    string message;
    string affected_routes?;
    string severity;
|};

// Initialize database
function initDatabase() returns error? {
    _ = check dbClient->execute(`
        CREATE TABLE IF NOT EXISTS service_disruptions (
            id VARCHAR(255) PRIMARY KEY,
            disruption_type VARCHAR(50) NOT NULL,
            title VARCHAR(255) NOT NULL,
            message TEXT NOT NULL,
            affected_routes TEXT,
            severity VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);

    log:printInfo("Admin database initialized");
}

// HTTP Service
service /admin on new http:Listener(9095) {

    // Health check
    resource function get health() returns string {
        return "Admin Service is running";
    }

    // ========== ROUTE MANAGEMENT ==========

    // Create route (proxies to transport service)
    resource function post routes(@http:Payload json routeData) 
            returns json|http:InternalServerError|error {
        
        http:Response response = check transportServiceClient->post("/transport/routes", routeData);
        
        if response.statusCode == 201 {
            log:printInfo("Route created by admin");
            return {"message": "Route created successfully"};
        }
        
        return <http:InternalServerError>{
            body: {"error": "Failed to create route"}
        };
    }

    // Get all routes
    resource function get routes() returns json|http:InternalServerError|error {
        json routes = check transportServiceClient->get("/transport/routes");
        return routes;
    }

    // Create trip (proxies to transport service)
    resource function post trips(@http:Payload json tripData) 
            returns json|http:InternalServerError|error {
        
        http:Response response = check transportServiceClient->post("/transport/trips", tripData);
        
        if response.statusCode == 201 {
            log:printInfo("Trip created by admin");
            return {"message": "Trip created successfully"};
        }
        
        return <http:InternalServerError>{
            body: {"error": "Failed to create trip"}
        };
    }

    // Update trip status
    resource function put trips/[string tripId]/status(@http:Payload json statusData) 
            returns json|http:InternalServerError|error {
        
        http:Response response = check transportServiceClient->put(
            "/transport/trips/" + tripId + "/status", 
            statusData
        );
        
        if response.statusCode == 200 {
            log:printInfo("Trip status updated by admin: " + tripId);
            return {"message": "Trip status updated successfully"};
        }
        
        return <http:InternalServerError>{
            body: {"error": "Failed to update trip status"}
        };
    }

    // ========== DISRUPTION MANAGEMENT ==========

    // Publish service disruption
    resource function post disruptions(@http:Payload DisruptionRequest disruptionReq) 
            returns http:Created|http:InternalServerError|error {
        
        string disruptionId = uuid:createType1AsString();
        
        // Save disruption to database
        sql:ExecutionResult result = check dbClient->execute(`
            INSERT INTO service_disruptions (id, disruption_type, title, message, affected_routes, severity)
            VALUES (${disruptionId}, ${disruptionReq.disruption_type}, ${disruptionReq.title}, 
                    ${disruptionReq.message}, ${disruptionReq.affected_routes}, ${disruptionReq.severity})
        `);

        // Publish disruption event to Kafka
        json disruptionEvent = {
            "disruption_id": disruptionId,
            "disruption_type": disruptionReq.disruption_type,
            "title": disruptionReq.title,
            "message": disruptionReq.message,
            "affected_routes": disruptionReq.affected_routes,
            "severity": disruptionReq.severity,
            "timestamp": time:utcNow()
        };

        check kafkaProducer->send({
            topic: "service-disruptions",
            value: disruptionEvent.toJsonString().toBytes()
        });

        log:printInfo("Service disruption published: " + disruptionReq.title);
        
        return http:CREATED;
    }

    // Get all disruptions
    resource function get disruptions() returns ServiceDisruption[]|http:InternalServerError|error {
        stream<ServiceDisruption, sql:Error?> disruptionStream = dbClient->query(
            `SELECT * FROM service_disruptions ORDER BY created_at DESC LIMIT 50`
        );
        
        ServiceDisruption[] disruptions = check from ServiceDisruption disruption in disruptionStream 
                                          select disruption;
        return disruptions;
    }

    // ========== REPORTS & ANALYTICS ==========

    // Get ticket sales report
    resource function get reports/tickets() returns json|http:InternalServerError|error {
        
        json tickets = check ticketingServiceClient->get("/ticketing/tickets");
        
        // In production, you would process this data for analytics
        // For now, return raw ticket data
        return {
            "report_type": "ticket_sales",
            "generated_at": time:utcToString(time:utcNow()),
            "data": tickets
        };
    }

    // Get payment reports
    resource function get reports/payments() returns json|http:InternalServerError|error {
        
        json payments = check paymentServiceClient->get("/payment/payments");
        
        return {
            "report_type": "payments",
            "generated_at": time:utcToString(time:utcNow()),
            "data": payments
        };
    }

    // Get passenger traffic report
    resource function get reports/traffic() returns json|http:InternalServerError|error {
        
        // Get all trips
        json trips = check transportServiceClient->get("/transport/trips");
        
        // Get all tickets
        json tickets = check ticketingServiceClient->get("/ticketing/tickets");
        
        return {
            "report_type": "passenger_traffic",
            "generated_at": time:utcToString(time:utcNow()),
            "total_trips": trips,
            "total_tickets": tickets
        };
    }

    // Get system health report
    resource function get reports/system-health() returns json|error {
        
        map<json> healthStatus = {};
        
        // Check each service
        http:Response|error passengerHealth = passengerServiceClient->get("/passenger/health");
        healthStatus["passenger_service"] = passengerHealth is http:Response ? "UP" : "DOWN";
        
        http:Response|error transportHealth = transportServiceClient->get("/transport/health");
        healthStatus["transport_service"] = transportHealth is http:Response ? "UP" : "DOWN";
        
        http:Response|error ticketingHealth = ticketingServiceClient->get("/ticketing/health");
        healthStatus["ticketing_service"] = ticketingHealth is http:Response ? "UP" : "DOWN";
        
        http:Response|error paymentHealth = paymentServiceClient->get("/payment/health");
        healthStatus["payment_service"] = paymentHealth is http:Response ? "UP" : "DOWN";
        
        http:Response|error notificationHealth = notificationServiceClient->get("/notification/health");
        healthStatus["notification_service"] = notificationHealth is http:Response ? "UP" : "DOWN";
        
        return {
            "report_type": "system_health",
            "generated_at": time:utcToString(time:utcNow()),
            "services": healthStatus
        };
    }

    // Get dashboard summary
    resource function get dashboard/summary() returns json|error {
        
        // Get counts from different services
        json tickets = check ticketingServiceClient->get("/ticketing/tickets");
        json payments = check paymentServiceClient->get("/payment/payments");
        json trips = check transportServiceClient->get("/transport/trips");
        json routes = check transportServiceClient->get("/transport/routes");
        
        return {
            "dashboard_type": "admin_summary",
            "generated_at": time:utcToString(time:utcNow()),
            "total_tickets": tickets is json[] ? tickets.length() : 0,
            "total_payments": payments is json[] ? payments.length() : 0,
            "total_trips": trips is json[] ? trips.length() : 0,
            "total_routes": routes is json[] ? routes.length() : 0
        };
    }
}

// Main function
public function main() returns error? {
    check initDatabase();
    log:printInfo("Admin Service started on port 9095");
}