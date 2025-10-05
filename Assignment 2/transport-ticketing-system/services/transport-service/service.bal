import ballerina/http;
import ballerina/log;
import ballerinax/mongodb;
import ballerina/uuid;
import ballerina/time;
import ballerinax/kafka;


configurable string mongoHost = "mongodb";
configurable int mongoPort = 27017;
configurable string mongoDatabase = "ticketing_system";


configurable string kafkaBootstrap = "kafka:9092";


final mongodb:Client mongoClient = check new ({
    connection: {
        serverAddress: {
            host: mongoHost,
            port: mongoPort
        }
    }
});


kafka:ProducerConfiguration producerConfig = {
    clientId: "transport-service-producer",
    acks: "all",
    retryCount: 3
};

final kafka:Producer kafkaProducer = check new (kafkaBootstrap, producerConfig);


type Route record {|
    string _id?;
    string routeId;
    string routeName;
    string vehicleType; 
    string startLocation;
    string endLocation;
    string[] stops;
    decimal distanceKm;
    boolean isActive;
    time:Utc createdAt;
    time:Utc updatedAt;
|};


type Trip record {|
    string _id?;
    string tripId;
    string routeId;
    time:Utc departureTime;
    time:Utc arrivalTime;
    string status; // "SCHEDULED", "ACTIVE", "COMPLETED", "CANCELLED", "DELAYED"
    int availableSeats;
    int totalSeats;
    decimal baseFare;
    time:Utc createdAt;
    time:Utc updatedAt;
|};


type CreateRouteRequest record {|
    string routeName;
    string vehicleType;
    string startLocation;
    string endLocation;
    string[] stops;
    decimal distanceKm;
|};

type CreateTripRequest record {|
    string routeId;
    string departureTime; 
    string arrivalTime; 
    int totalSeats;
    decimal baseFare;
|};

type UpdateTripStatusRequest record {|
    string status;
    string? reason;
|};

type UpdateRouteRequest record {|
    string? routeName;
    boolean? isActive;
    string[]? stops;
|};

type RouteResponse record {|
    string routeId;
    string routeName;
    string vehicleType;
    string startLocation;
    string endLocation;
    string[] stops;
    decimal distanceKm;
    boolean isActive;
    string createdAt;
    string updatedAt;
|};

type TripResponse record {|
    string tripId;
    string routeId;
    string departureTime;
    string arrivalTime;
    string status;
    int availableSeats;
    int totalSeats;
    decimal baseFare;
    string createdAt;
    RouteInfo? routeInfo;
|};

type RouteInfo record {|
    string routeName;
    string vehicleType;
    string startLocation;
    string endLocation;
    string[] stops;
|};

type ScheduleUpdateEvent record {|
    string eventId;
    string eventType; // "TRIP_CANCELLED", "TRIP_DELAYED", "TRIP_RESCHEDULED"
    string tripId;
    string routeId;
    string message;
    string timestamp;
|};

type ErrorResponse record {|
    string message;
    string ei_code?;
|};


listener http:Listener transportListener = new (8082);


service /api/transport on transportListener {

    
    resource function get health() returns json {
        return {
            status: "UP",
            serviceName: "Transport Service",
            timestamp: time:utcToString(time:utcNow())
        };
    }

    // ==================== ROUTE MANAGEMENT ====================

    
    resource function post routes(CreateRouteRequest request) returns http:Created|http:BadRequest|http:InternalServerError {
        do {
            
            if request.vehicleType != "bus" && request.vehicleType != "train" {
                return <http:BadRequest>{
                    body: {
                        message: "Invalid vehicle type. Must be 'bus' or 'train'"
                    }
                };
            }

            
            if request.stops.length() < 2 {
                return <http:BadRequest>{
                    body: {
                        message: "Route must have at least 2 stops"
                    }
                };
            }

            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection routes = check db->getCollection("routes");

            string routeId = uuid:createType1AsString();
            time:Utc now = time:utcNow();

            Route newRoute = {
                routeId: routeId,
                routeName: request.routeName,
                vehicleType: request.vehicleType,
                startLocation: request.startLocation,
                endLocation: request.endLocation,
                stops: request.stops,
                distanceKm: request.distanceKm,
                isActive: true,
                createdAt: now,
                updatedAt: now
            };

            check routes->insertOne(newRoute);

            log:printInfo(string `Route created: ${request.routeName} (${routeId})`);

            return <http:Created>{
                headers: {
                    "Location": string `/api/transport/routes/${routeId}`
                },
                body: {
                    message: "Route created successfully",
                    routeId: routeId,
                    routeName: request.routeName
                }
            };

        } on fail var e {
            log:printError("Failed to create route", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to create route",
                    ei_code: e.message()
                }
            };
        }
    }

    
    resource function get routes(string? vehicleType = (), boolean? activeOnly = true) returns RouteResponse[]|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection routes = check db->getCollection("routes");

            map<json> filter = {};

          
            if activeOnly == true {
                filter["isActive"] = true;
            }

            
            if vehicleType is string {
                if vehicleType != "bus" && vehicleType != "train" {
                    filter["vehicleType"] = "invalid"; // Will return empty array
                } else {
                    filter["vehicleType"] = vehicleType;
                }
            }

            stream<Route, error?> routeStream = check routes->find(filter);
            RouteResponse[] routeResponses = [];

            check from Route route in routeStream
                do {
                    routeResponses.push({
                        routeId: route.routeId,
                        routeName: route.routeName,
                        vehicleType: route.vehicleType,
                        startLocation: route.startLocation,
                        endLocation: route.endLocation,
                        stops: route.stops,
                        distanceKm: route.distanceKm,
                        isActive: route.isActive,
                        createdAt: time:utcToString(route.createdAt),
                        updatedAt: time:utcToString(route.updatedAt)
                    });
                };

            log:printInfo(string `Retrieved ${routeResponses.length()} routes`);

            return routeResponses;

        } on fail var e {
            log:printError("Failed to get routes", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve routes",
                    ei_code: e.message()
                }
            };
        }
    }

    
    resource function get routes/[string routeId]() returns RouteResponse|http:NotFound|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection routes = check db->getCollection("routes");

            map<json> filter = {"routeId": routeId};
            stream<Route, error?> routeStream = check routes->find(filter);
            Route[] results = check from Route r in routeStream select r;

            if results.length() == 0 {
                return <http:NotFound>{
                    body: {
                        message: "Route not found",
                        routeId: routeId
                    }
                };
            }

            Route route = results[0];

            return {
                routeId: route.routeId,
                routeName: route.routeName,
                vehicleType: route.vehicleType,
                startLocation: route.startLocation,
                endLocation: route.endLocation,
                stops: route.stops,
                distanceKm: route.distanceKm,
                isActive: route.isActive,
                createdAt: time:utcToString(route.createdAt),
                updatedAt: time:utcToString(route.updatedAt)
            };

        } on fail var e {
            log:printError("Failed to get route", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve route",
                    ei_code: e.message()
                }
            };
        }
    }

    
    resource function put routes/[string routeId](UpdateRouteRequest request) returns RouteResponse|http:NotFound|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection routes = check db->getCollection("routes");

            
            map<json> filter = {"routeId": routeId};
            stream<Route, error?> routeStream = check routes->find(filter);
            Route[] results = check from Route r in routeStream select r;

            if results.length() == 0 {
                return <http:NotFound>{
                    body: {
                        message: "Route not found",
                        routeId: routeId
                    }
                };
            }

            
            map<json> updateDoc = {
                "updatedAt": time:utcNow()
            };

            if request.routeName is string {
                updateDoc["routeName"] = request.routeName;
            }
            if request.isActive is boolean {
                updateDoc["isActive"] = request.isActive;
            }
            if request.stops is string[] {
                updateDoc["stops"] = request.stops;
            }

            mongodb:Update update = {"$set": updateDoc};
            mongodb:UpdateResult updateResult = check routes->updateOne(filter, update);

            log:printInfo(string `Route updated: ${routeId}`);

            
            stream<Route, error?> updatedStream = check routes->find(filter);
            Route[] updatedResults = check from Route r in updatedStream select r;
            Route updated = updatedResults[0];

            return {
                routeId: updated.routeId,
                routeName: updated.routeName,
                vehicleType: updated.vehicleType,
                startLocation: updated.startLocation,
                endLocation: updated.endLocation,
                stops: updated.stops,
                distanceKm: updated.distanceKm,
                isActive: updated.isActive,
                createdAt: time:utcToString(updated.createdAt),
                updatedAt: time:utcToString(updated.updatedAt)
            };

        } on fail var e {
            log:printError("Failed to update route", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to update route",
                    ei_code: e.message()
                }
            };
        }
    }

    // ==================== TRIP MANAGEMENT ====================

    
    resource function post trips(CreateTripRequest request) returns http:Created|http:BadRequest|http:NotFound|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection trips = check db->getCollection("trips");
            mongodb:Collection routes = check db->getCollection("routes");

            
            map<json> routeFilter = {"routeId": request.routeId, "isActive": true};
            stream<Route, error?> routeStream = check routes->find(routeFilter);
            Route[] routeResults = check from Route r in routeStream select r;

            if routeResults.length() == 0 {
                return <http:NotFound>{
                    body: {
                        message: "Route not found or inactive",
                        routeId: request.routeId
                    }
                };
            }

            
            if request.totalSeats <= 0 {
                return <http:BadRequest>{
                    body: {
                        message: "Total seats must be greater than 0"
                    }
                };
            }

            
            if request.baseFare <= 0 {
                return <http:BadRequest>{
                    body: {
                        message: "Base fare must be greater than 0"
                    }
                };
            }

            
            time:Utc departTime = check time:utcFromString(request.departureTime);
            time:Utc arriveTime = check time:utcFromString(request.arrivalTime);

            
            if time:utcDiffSeconds(arriveTime, departTime) <= 0.0d {
                return <http:BadRequest>{
                    body: {
                        message: "Arrival time must be after departure time"
                    }
                };
            }

            string tripId = uuid:createType1AsString();
            time:Utc now = time:utcNow();

            Trip newTrip = {
                tripId: tripId,
                routeId: request.routeId,
                departureTime: departTime,
                arrivalTime: arriveTime,
                status: "SCHEDULED",
                availableSeats: request.totalSeats,
                totalSeats: request.totalSeats,
                baseFare: request.baseFare,
                createdAt: now,
                updatedAt: now
            };

            check trips->insertOne(newTrip);

            log:printInfo(string `Trip created: ${tripId} for route ${request.routeId}`);

            return <http:Created>{
                headers: {
                    "Location": string `/api/transport/trips/${tripId}`
                },
                body: {
                    message: "Trip created successfully",
                    tripId: tripId,
                    routeId: request.routeId
                }
            };

        } on fail var e {
            log:printError("Failed to create trip", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to create trip",
                    ei_code: e.message()
                }
            };
        }
    }

    
    resource function get trips(string? routeId = (), string? status = ()) returns TripResponse[]|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection trips = check db->getCollection("trips");

            map<json> filter = {};

            if routeId is string {
                filter["routeId"] = routeId;
            }

            if status is string {
                
                string[] validStatuses = ["SCHEDULED", "ACTIVE", "COMPLETED", "CANCELLED", "DELAYED"];
                if validStatuses.indexOf(status) is int {
                    filter["status"] = status;
                }
            }

            stream<Trip, error?> tripStream = check trips->find(filter);
            TripResponse[] tripResponses = [];

            check from Trip trip in tripStream
                do {
                    tripResponses.push({
                        tripId: trip.tripId,
                        routeId: trip.routeId,
                        departureTime: time:utcToString(trip.departureTime),
                        arrivalTime: time:utcToString(trip.arrivalTime),
                        status: trip.status,
                        availableSeats: trip.availableSeats,
                        totalSeats: trip.totalSeats,
                        baseFare: trip.baseFare,
                        createdAt: time:utcToString(trip.createdAt),
                        routeInfo: ()
                    });
                };

            log:printInfo(string `Retrieved ${tripResponses.length()} trips`);

            return tripResponses;

        } on fail var e {
            log:printError("Failed to get trips", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve trips",
                    ei_code: e.message()
                }
            };
        }
    }

    
    resource function get trips/[string tripId]() returns TripResponse|http:NotFound|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection trips = check db->getCollection("trips");
            mongodb:Collection routes = check db->getCollection("routes");

            map<json> filter = {"tripId": tripId};
            stream<Trip, error?> tripStream = check trips->find(filter);
            Trip[] results = check from Trip t in tripStream select t;

            if results.length() == 0 {
                return <http:NotFound>{
                    body: {
                        message: "Trip not found",
                        tripId: tripId
                    }
                };
            }

            Trip trip = results[0];

            
            map<json> routeFilter = {"routeId": trip.routeId};
            stream<Route, error?> routeStream = check routes->find(routeFilter);
            Route[] routeResults = check from Route r in routeStream select r;

            RouteInfo? routeInfo = ();
            if routeResults.length() > 0 {
                Route route = routeResults[0];
                routeInfo = {
                    routeName: route.routeName,
                    vehicleType: route.vehicleType,
                    startLocation: route.startLocation,
                    endLocation: route.endLocation,
                    stops: route.stops
                };
            }

            return {
                tripId: trip.tripId,
                routeId: trip.routeId,
                departureTime: time:utcToString(trip.departureTime),
                arrivalTime: time:utcToString(trip.arrivalTime),
                status: trip.status,
                availableSeats: trip.availableSeats,
                totalSeats: trip.totalSeats,
                baseFare: trip.baseFare,
                createdAt: time:utcToString(trip.createdAt),
                routeInfo: routeInfo
            };

        } on fail var e {
            log:printError("Failed to get trip", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve trip",
                    ei_code: e.message()
                }
            };
        }
    }

    
    resource function put trips/[string tripId]/status(UpdateTripStatusRequest request) returns http:Ok|http:BadRequest|http:NotFound|http:InternalServerError {
        do {
            string newStatus = request.status;

            
            string[] validStatuses = ["SCHEDULED", "ACTIVE", "COMPLETED", "CANCELLED", "DELAYED"];
            if validStatuses.indexOf(newStatus) is () {
                return <http:BadRequest>{
                    body: {
                        message: string `Invalid status. Must be one of: ${string:'join(", ", ...validStatuses)}`
                    }
                };
            }

            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection trips = check db->getCollection("trips");

            
            map<json> filter = {"tripId": tripId};
            stream<Trip, error?> tripStream = check trips->find(filter);
            Trip[] results = check from Trip t in tripStream select t;

            if results.length() == 0 {
                return <http:NotFound>{
                    body: {
                        message: "Trip not found",
                        tripId: tripId
                    }
                };
            }

            Trip existingTrip = results[0];
            string oldStatus = existingTrip.status;

            
            mongodb:Update update = {
                "$set": {
                    "status": newStatus,
                    "updatedAt": time:utcNow()
                }
            };

            mongodb:UpdateResult updateResult = check trips->updateOne(filter, update);

            if updateResult.modifiedCount == 0 {
                return <http:Ok>{
                    body: {
                        message: "No changes made - status already set",
                        tripId: tripId,
                        currentStatus: oldStatus
                    }
                };
            }

            
            string eventType = "TRIP_STATUS_CHANGED";
            if newStatus == "CANCELLED" {
                eventType = "TRIP_CANCELLED";
            } else if newStatus == "DELAYED" {
                eventType = "TRIP_DELAYED";
            } else if newStatus == "SCHEDULED" {
                eventType = "TRIP_RESCHEDULED";
            }

            
            ScheduleUpdateEvent event = {
                eventId: uuid:createType1AsString(),
                eventType: eventType,
                tripId: tripId,
                routeId: existingTrip.routeId,
                message: request.reason ?: string `Trip status changed from ${oldStatus} to ${newStatus}`,
                timestamp: time:utcToString(time:utcNow())
            };

            error? sendResult = kafkaProducer->send({
                topic: "schedule.updates",
                value: event.toJsonString()
            });

            if sendResult is error {
                log:printError("Failed to publish schedule update event", 'error = sendResult);
            } else {
                log:printInfo(string `Schedule update event published for trip ${tripId}`);
            }

            log:printInfo(string `Trip ${tripId} status updated: ${oldStatus} -> ${newStatus}`);

            return <http:Ok>{
                body: {
                    message: "Trip status updated successfully",
                    tripId: tripId,
                    previousStatus: oldStatus,
                    currentStatus: newStatus,
                    eventPublished: sendResult is ()
                }
            };

        } on fail var e {
            log:printError("Failed to update trip status", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to update trip status",
                    ei_code: e.message()
                }
            };
        }
    }

    
    resource function post trips/[string tripId]/reserve() returns http:Ok|http:BadRequest|http:NotFound|http:Conflict|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection trips = check db->getCollection("trips");

            map<json> filter = {"tripId": tripId};
            stream<Trip, error?> tripStream = check trips->find(filter);
            Trip[] results = check from Trip t in tripStream select t;

            if results.length() == 0 {
                return <http:NotFound>{
                    body: {
                        message: "Trip not found",
                        tripId: tripId
                    }
                };
            }

            Trip trip = results[0];

            
            if trip.status != "SCHEDULED" {
                return <http:BadRequest>{
                    body: {
                        message: string `Cannot reserve seat. Trip status is ${trip.status}`,
                        tripId: tripId
                    }
                };
            }

            
            if trip.availableSeats <= 0 {
                return <http:Conflict>{
                    body: {
                        message: "No seats available",
                        tripId: tripId,
                        availableSeats: 0
                    }
                };
            }

            // Atomically decrement available
            int newAvailableSeats = trip.availableSeats - 1;
            mongodb:Update update = {
                "$set": {
                    "availableSeats": newAvailableSeats,
                    "updatedAt": time:utcNow()
                }
            };

            
            map<json> atomicFilter = {"tripId": tripId, "availableSeats": trip.availableSeats};
            mongodb:UpdateResult result = check trips->updateOne(atomicFilter, update);

            if result.modifiedCount == 0 {
                
                return <http:Conflict>{
                    body: {
                        message: "Seat reservation failed due to concurrent request",
                        tripId: tripId
                    }
                };
            }

            log:printInfo(string `Seat reserved for trip ${tripId}. Remaining: ${newAvailableSeats}`);

            return <http:Ok>{
                body: {
                    message: "Seat reserved successfully",
                    tripId: tripId,
                    availableSeats: newAvailableSeats,
                    totalSeats: trip.totalSeats
                }
            };

        } on fail var e {
            log:printError("Failed to reserve seat", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to reserve seat",
                    ei_code: e.message()
                }
            };
        }
    }

    
    resource function post trips/[string tripId]/release() returns http:Ok|http:BadRequest|http:NotFound|http:InternalServerError {
        do {
            mongodb:Database db = check mongoClient->getDatabase(mongoDatabase);
            mongodb:Collection trips = check db->getCollection("trips");

            map<json> filter = {"tripId": tripId};
            stream<Trip, error?> tripStream = check trips->find(filter);
            Trip[] results = check from Trip t in tripStream select t;

            if results.length() == 0 {
                return <http:NotFound>{
                    body: {
                        message: "Trip not found",
                        tripId: tripId
                    }
                };
            }

            Trip trip = results[0];

            
            if trip.availableSeats >= trip.totalSeats {
                return <http:BadRequest>{
                    body: {
                        message: "Cannot release seat. All seats are already available",
                        tripId: tripId
                    }
                };
            }

            int newAvailableSeats = trip.availableSeats + 1;
            mongodb:Update update = {
                "$set": {
                    "availableSeats": newAvailableSeats,
                    "updatedAt": time:utcNow()
                }
            };

            mongodb:UpdateResult result = check trips->updateOne(filter, update);

            log:printInfo(string `Seat released for trip ${tripId}. Available: ${newAvailableSeats}`);

            return <http:Ok>{
                body: {
                    message: "Seat released successfully",
                    tripId: tripId,
                    availableSeats: newAvailableSeats,
                    totalSeats: trip.totalSeats
                }
            };

        } on fail var e {
            log:printError("Failed to release seat", 'error = e);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to release seat",
                    ei_code: e.message()
                }
            };
        }
    }
}
