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

    // Delete an asset
    resource function delete assets/[string assetTag]() returns Asset|error {
        Asset? asset = assetsTable[assetTag];
        if (asset is ()) {
            return error("Asset not found with this tag");
        }
        Asset removedAsset = assetsTable.remove(assetTag);
        return removedAsset;
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
    //mbanga-workorder-management
    // Add work order to asset
    resource function post assets/[string assetTag]/workorders(@http:Payload WorkOrder workOrder) returns WorkOrder|error {
        Asset? assetOpt = assetsTable[assetTag];
        if (assetOpt is ()) {
            return error("Asset not found with this tag");
        }
        
        Asset asset = assetOpt;
        asset.workOrders[workOrder.workOrderId] = workOrder;
        assetsTable.put(asset);
        return workOrder;
    }

    // Update work order status
    resource function put assets/[string assetTag]/workorders/[string workOrderId](@http:Payload WorkOrder updatedWorkOrder) returns WorkOrder|error {
        Asset? assetOpt = assetsTable[assetTag];
        if (assetOpt is ()) {
            return error("Asset not found with this tag");
        }
        
        Asset asset = assetOpt;
        if (!asset.workOrders.hasKey(workOrderId)) {
            return error("Work order not found");
        }
        
        asset.workOrders[workOrderId] = updatedWorkOrder;
        assetsTable.put(asset);
        return updatedWorkOrder;
    }

    // Add task to work order
    resource function post assets/[string assetTag]/workorders/[string workOrderId]/tasks(@http:Payload Task task) returns Task|error {
        Asset? assetOpt = assetsTable[assetTag];
        if (assetOpt is ()) {
            return error("Asset not found with this tag");
        }
        
        Asset asset = assetOpt;
        WorkOrder? workOrderOpt = asset.workOrders[workOrderId];
        if (workOrderOpt is ()) {
            return error("Work order not found");
        }
        
        WorkOrder workOrder = workOrderOpt;
        workOrder.tasks.push(task);
        asset.workOrders[workOrderId] = workOrder;
        assetsTable.put(asset);
        return task;
    }

    // Remove task from work order
    resource function delete assets/[string assetTag]/workorders/[string workOrderId]/tasks/[string taskId]() returns Task|error {
        Asset? assetOpt = assetsTable[assetTag];
        if (assetOpt is ()) {
            return error("Asset not found with this tag");
        }
        
        Asset asset = assetOpt;
        WorkOrder? workOrderOpt = asset.workOrders[workOrderId];
        if (workOrderOpt is ()) {
            return error("Work order not found");
        }
        
        WorkOrder workOrder = workOrderOpt;
        Task? removedTask = ();
        
        foreach int i in 0...workOrder.tasks.length()-1 {
            if (workOrder.tasks[i].taskId == taskId) {
                removedTask = workOrder.tasks.remove(i);
                break;
            }
        }
        
        if (removedTask is ()) {
            return error("Task not found");
        }
        
        asset.workOrders[workOrderId] = workOrder;
        assetsTable.put(asset);
        return <Task>removedTask;
    }
}
