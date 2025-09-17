import ballerina/http;
import ballerina/time;

type Component record {
    string componentId;
    string name;
    string description;
};

type Schedule record {
    string scheduleId;
    string frequency; // "QUARTERLY", "YEARLY", etc.
    string nextDueDate; // ISO date format
    string description;
};

type Task record {
    string taskId;
    string description;
    string status; // "PENDING", "IN_PROGRESS", "COMPLETED"
};

type WorkOrder record {
    string workOrderId;
    string description;
    string status; // "OPEN", "IN_PROGRESS", "CLOSED"
    string dateOpened;
    string? dateClosed;
    Task[] tasks;
};

type Asset record {
    readonly string assetTag;
    string name;
    string faculty;
    string department;
    string status; // "ACTIVE", "UNDER_REPAIR", "DISPOSED"
    string acquiredDate;
    map<Component> components;
    map<Schedule> schedules;
    map<WorkOrder> workOrders;
};

table<Asset> key(assetTag) assetsTable = table [];

// Add CORS configuration for web interface
@http:ServiceConfig {
    cors: {
        allowOrigins: ["http://localhost:3000", "http://127.0.0.1:3000", "file://", "null"],
        allowCredentials: false,
        allowHeaders: ["Content-Type", "Authorization"],
        allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    }
}
service /asset_management on new http:Listener(9090) {

    // Handle preflight OPTIONS requests
    resource function options .(http:Request req) returns http:Response {
        http:Response response = new;
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
        response.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
        response.statusCode = 200;
        return response;
    }

    // Create a new asset
    resource function post assets(Asset newAsset) returns Asset|error {
        if (assetsTable.hasKey(newAsset.assetTag)) {
            return error("Asset already exists with this tag");
        }
        assetsTable.put(newAsset);
        return newAsset;
    }

    //henry------------------------------------------------------------
       // Get all assets
    resource function get assets() returns Asset[] {
        Asset[] allAssets = [];
        foreach Asset asset in assetsTable {
            allAssets.push(asset);
        }
        return allAssets;
    }



    // Get assets by faculty
    resource function get assets/faculty/[string faculty]() returns Asset[]|error {
        Asset[] facultyAssets = [];
        
        foreach Asset asset in assetsTable {
            if (asset.faculty == faculty) {
                facultyAssets.push(asset);
            }
        }
        
        if (facultyAssets.length() == 0) {
            return error("No assets found for this faculty");
        }
        return facultyAssets;
    }
 //henry🤔🤔🤔🤔🤔coding is not fun at all-------------
 
     
    // Returns all assets with maintenance schedules that are overdue
 resource function get assets/overdue() returns Asset[]|error {
        Asset[] overdueAssets = [];
        time:Utc currentTime = time:utcNow();
        
        foreach Asset asset in assetsTable {
            foreach Schedule schedule in asset.schedules {
                time:Civil|error dueDate = time:civilFromString(schedule.nextDueDate);
                if (dueDate is time:Civil) {
                    time:Utc|error dueDateUtc = time:utcFromCivil(dueDate);
                    if (dueDateUtc is time:Utc) {
                        decimal diffSeconds = time:utcDiffSeconds(currentTime, dueDateUtc);
                        if (diffSeconds > 0.0d) {
                            overdueAssets.push(asset);
                            break; // No need to check other schedules for this asset
                        }
                    }
                }
            }
        }
        
 // ...existing code...
        return overdueAssets;
    }
}