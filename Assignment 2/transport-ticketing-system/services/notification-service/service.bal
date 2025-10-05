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

// Kafka consumer for multiple topics
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "notification-service-group",
    topics: ["ticket-validated", "schedule-updates", "service-disruptions", "login-events"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: true
};

listener kafka:Listener kafkaConsumer = new (kafkaBootstrapServers, consumerConfig);

// Types
type Notification record {|
    string id?;
    string user_id?;
    string notification_type;
    string title;
    string message;
    string event_data?;
    boolean is_read;
    string created_at?;
|};

// Initialize database
function initDatabase() returns error? {
    _ = check dbClient->execute(`
        CREATE TABLE IF NOT EXISTS notifications (
            id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255),
            notification_type VARCHAR(50) NOT NULL,
            title VARCHAR(255) NOT NULL,
            message TEXT NOT NULL,
            event_data TEXT,
            is_read BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);

    log:printInfo("Notification database initialized");
}

// Send notification (save to database and log)
function sendNotification(string? userId, string notificationType, string title, string message, json eventData) returns error? {
    string notificationId = uuid:createType1AsString();
    
    sql:ExecutionResult result = check dbClient->execute(`
        INSERT INTO notifications (id, user_id, notification_type, title, message, event_data, is_read)
        VALUES (${notificationId}, ${userId}, ${notificationType}, ${title}, ${message}, 
                ${eventData.toJsonString()}, false)
    `);

    // Log notification (in production, this would send email/SMS/push notification)
    log:printInfo("=== NOTIFICATION SENT ===");
    log:printInfo("Type: " + notificationType);
    log:printInfo("To User: " + (userId ?: "ALL USERS"));
    log:printInfo("Title: " + title);
    log:printInfo("Message: " + message);
    log:printInfo("========================");
}

// HTTP Service
service /notification on new http:Listener(9094) {

    // Health check
    resource function get health() returns string {
        return "Notification Service is running";
    }

    // Get user's notifications
    resource function get users/[string userId]/notifications() 
            returns Notification[]|http:InternalServerError|error {
        
        stream<Notification, sql:Error?> notificationStream = dbClient->query(
            `SELECT * FROM notifications WHERE user_id = ${userId} ORDER BY created_at DESC LIMIT 50`
        );
        
        Notification[] notifications = check from Notification notification in notificationStream 
                                        select notification;
        return notifications;
    }

    // Mark notification as read
    resource function put notifications/[string notificationId]/read() 
            returns http:Ok|http:NotFound|http:InternalServerError|error {
        
        sql:ExecutionResult result = check dbClient->execute(`
            UPDATE notifications SET is_read = true WHERE id = ${notificationId}
        `);

        if result.affectedRowCount == 0 {
            return http:NOT_FOUND;
        }

        return http:OK;
    }

    // Get all notifications (for admin)
    resource function get notifications() returns Notification[]|http:InternalServerError|error {
        stream<Notification, sql:Error?> notificationStream = dbClient->query(
            `SELECT * FROM notifications ORDER BY created_at DESC LIMIT 100`
        );
        
        Notification[] notifications = check from Notification notification in notificationStream 
                                        select notification;
        return notifications;
    }

    // Get unread notification count for user
    resource function get users/[string userId]/notifications/unread/count() 
            returns json|http:InternalServerError|error {
        
        sql:ParameterizedQuery query = `SELECT COUNT(*) as count FROM notifications 
                                        WHERE user_id = ${userId} AND is_read = false`;
        
        stream<record {| int count; |}, sql:Error?> resultStream = dbClient->query(query);
        record {| int count; |}[] results = check from var result in resultStream select result;
        
        int unreadCount = 0;
        if results.length() > 0 {
            unreadCount = results[0].count;
        }

        return {
            "user_id": userId,
            "unread_count": unreadCount
        };
    }
}

// Kafka consumer service
service on kafkaConsumer {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        
        foreach kafka:ConsumerRecord kafkaRecord in records {
            byte[] value = kafkaRecord.value;
            string message = check string:fromBytes(value);
            json eventData = check message.fromJsonString();

            string topic = kafkaRecord.topic;

            if topic == "ticket-validated" {
                // Ticket validation notification
                string userId = check eventData.user_id;
                string ticketId = check eventData.ticket_id;
                
                check sendNotification(
                    userId,
                    "ticket_validated",
                    "Ticket Validated Successfully",
                    "Your ticket " + ticketId + " has been validated. Have a safe journey!",
                    eventData
                );

            } else if topic == "schedule-updates" {
                // Schedule update notification
                string eventType = check eventData.event_type;
                
                if eventType == "trip_created" {
                    string routeId = check eventData.route_id;
                    check sendNotification(
                        (),
                        "schedule_update",
                        "New Trip Scheduled",
                        "A new trip has been scheduled for route " + routeId,
                        eventData
                    );
                } else if eventType == "trip_status_updated" {
                    string tripId = check eventData.trip_id;
                    string newStatus = check eventData.new_status;
                    check sendNotification(
                        (),
                        "schedule_update",
                        "Trip Status Update",
                        "Trip " + tripId + " status changed to: " + newStatus,
                        eventData
                    );
                }

            } else if topic == "service-disruptions" {
                // Service disruption notification
                string disruptionType = check eventData.disruption_type;
                string disruption_message = check eventData.message;
                
                check sendNotification(
                    (),
                    "service_disruption",
                    "Service Disruption Alert",
                    disruption_message,
                    eventData
                );

            } else if topic == "login-events" {
                // Login notification (optional - for security)
                string userId = check eventData.user_id;
                string username = check eventData.username;
                
                check sendNotification(
                    userId,
                    "login_event",
                    "Login Detected",
                    "Your account " + username + " was accessed.",
                    eventData
                );
            }
        }
    }
}

// Main function
public function main() returns error? {
    check initDatabase();
    log:printInfo("Notification Service started on port 9094");
    log:printInfo("Kafka consumer started for notification events");
    
    // Keep the service running
    runtime:sleep(3600);
}