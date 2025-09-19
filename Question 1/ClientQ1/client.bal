import ballerina/io;
import ballerina/http;

type Component record {
    string componentId;
    string name;
    string description;
};

type Schedule record {
    string scheduleId;
    string frequency;
    string nextDueDate;
    string description;
};

type Task record {
    string taskId;
    string description;
    string status;
};

type WorkOrder record {
    string workOrderId;
    string description;
    string status;
    string dateOpened;
    string? dateClosed;
    Task[] tasks;
};

type Asset record {
    readonly string assetTag;
    string name;
    string faculty;
    string department;
    string status;
    string acquiredDate;
    map<Component> components;
    map<Schedule> schedules;
    map<WorkOrder> workOrders;
};

http:Client client_asset = check new ("http://localhost:9090/asset_management");

public function main() returns error? {
    io:println("NUST Facilities Directorate Asset Management System");
    io:println("================================================\n");
    io:println("Choose an action from below with specified digit");
    io:println("1. Add a new asset"); //Murphy
    io:println("2. View all assets"); //Henry
    io:println("3. Update an existing asset"); //Mutombo
    io:println("4. View specific asset by tag"); //Patrick
    io:println("5. Delete an asset"); //Mbanga
    io:println("6. View assets by faculty"); //Murphy Sisamu
    io:println("7. Check for overdue maintenance");//Henry
    io:println("8. Manage components"); //Mutombo
    io:println("9. Manage schedules"); //Patrick
    io:println("10. Manage work orders"); //Mbanga
    io:println("11. Exit");
    
    while true {
        string cli = io:readln("Choose an action (1-11)> ");
        if cli == "11" {
            io:println("Goodbye!");
            break;
        }
        _ = check CLI(cli);
    }
}

function CLI(string cli) returns error? {
    match cli {
        "1" => {
            string assetTag = io:readln("Asset Tag: ");
            string name = io:readln("Asset Name: ");
            string faculty = io:readln("Faculty: ");
            string department = io:readln("Department: ");
            string status = io:readln("Status (ACTIVE/UNDER_REPAIR/DISPOSED): ");
            string acquiredDate = io:readln("Acquired Date (YYYY-MM-DD): ");
            
            Asset asset = {
                assetTag: assetTag,
                name: name,
                faculty: faculty,
                department: department,
                status: status,
                acquiredDate: acquiredDate,
                components: {},
                schedules: {},
                workOrders: {}
            };
            
            Asset assetResp = check client_asset->/assets.post(asset);
            io:println("Asset created successfully:");
            io:println(assetResp.toJsonString());
            io:println("================================================\n");
        }

        //henry----------------------------------------------------------
        "2" => {
            Asset[] assets = check client_asset->/assets;
            io:println("All Assets:");
            io:println("===========");
            foreach Asset asset in assets {
                io:println(asset.toJsonString());
                io:println("================================================\n");
            }
        }
"3"   
// Update an existing asset
    resource function put assets/[string assetTag](@http:Payload Asset updatedAsset) returns Asset|error {
        Asset? existingAssetOpt = assetsTable[assetTag];
        
        if (existingAssetOpt is ()) {
            return error("Asset not found with this tag");
        }
        
        Asset existingAsset = existingAssetOpt;
        
        // Update fields
        existingAsset.name = updatedAsset.name;
        existingAsset.faculty = updatedAsset.faculty;
        existingAsset.department = updatedAsset.department;
        existingAsset.status = updatedAsset.status;
        existingAsset.acquiredDate = updatedAsset.acquiredDate;
        
        assetsTable.put(existingAsset);
        return existingAsset;
    }
             "4" => {
            string assetTag = io:readln("Asset Tag: ");
            Asset asset = check client_asset->/assets/[assetTag];
            io:println("Asset Details:");
            io:println(asset.toJsonString());
        }

         
        "6" => {
            string faculty = io:readln("Faculty: ");
            Asset[] assets = check client_asset->/assets/faculty/[faculty];
            io:println("Assets in faculty: " + faculty);
            foreach Asset asset in assets {
                io:println(asset.toJsonString());
            }
        }

       //henry🤔🤔🤔🤔🤔coding is not fun at all------------------ 
       // Fetch and display all assets with overdue maintenance

       "7" => {
            Asset[] overdueAssets = check client_asset->/assets/overdue;
            io:println("Assets with overdue maintenance:");
            if (overdueAssets.length() > 0) {
                foreach Asset asset in overdueAssets {
                    io:println(asset.toJsonString());
                }
            } else {
                io:println("No assets with overdue maintenance!");
            }
        }

        _ => {
            io:println("Invalid option. Please choose a number between 1-11.");
        }
"8"
            io:println("Component Management");
            io:println("1. Add component");
            io:println("2. Remove component");
            string choice = io:readln("Choose (1-2): ");
            
            if choice == "1" {
                string assetTag = io:readln("Asset Tag: ");
                string componentId = io:readln("Component ID: ");
                string name = io:readln("Component Name: ");
                string description = io:readln("Component Description: ");
                
                Component component = {
                    componentId: componentId,
                    name: name,
                    description: description
                };
                
                Component addedComponent = check client_asset->/assets/[assetTag]/components.post(component);
                io:println("Component added successfully:");
                io:println(addedComponent.toJsonString());
            } else if choice == "2" {
                string assetTag = io:readln("Asset Tag: ");
                string componentId = io:readln("Component ID to remove: ");
                
                Component removedComponent = check client_asset->/assets/[assetTag]/components/[componentId].delete();
                io:println("Component removed successfully:");
                io:println(removedComponent.toJsonString());
            }
        }

        "9" => {
            io:println("Schedule Management");
            io:println("1. Add schedule");
            io:println("2. Remove schedule");
            string choice = io:readln("Choose (1-2): ");
            
            if choice == "1" {
                string assetTag = io:readln("Asset Tag: ");
                string scheduleId = io:readln("Schedule ID: ");
                string frequency = io:readln("Frequency (QUARTERLY/YEARLY): ");
                string nextDueDate = io:readln("Next Due Date (YYYY-MM-DD): ");
                string description = io:readln("Description: ");
                
                Schedule schedule = {
                    scheduleId: scheduleId,
                    frequency: frequency,
                    nextDueDate: nextDueDate,
                    description: description
                };
                
                Schedule addedSchedule = check client_asset->/assets/[assetTag]/schedules.post(schedule);
                io:println("Schedule added successfully:");
                io:println(addedSchedule.toJsonString());
            } else if choice == "2" {
                string assetTag = io:readln("Asset Tag: ");
                string scheduleId = io:readln("Schedule ID to remove: ");
                
                Schedule removedSchedule = check client_asset->/assets/[assetTag]/schedules/[scheduleId].delete();
                io:println("Schedule removed successfully:");
                io:println(removedSchedule.toJsonString());
            }
        }
        "10" => {
            io:println("Work Order Management");
            io:println("1. Add work order");
            io:println("2. Update work order status");
            io:println("3. Add task to work order");
            string choice = io:readln("Choose (1-3): ");
            
            if choice == "1" {
                string assetTag = io:readln("Asset Tag: ");
                string workOrderId = io:readln("Work Order ID: ");
                string description = io:readln("Description: ");
                string status = io:readln("Status (OPEN/IN_PROGRESS/CLOSED): ");
                string dateOpened = io:readln("Date Opened (YYYY-MM-DD): ");
                
                WorkOrder workOrder = {
                    workOrderId: workOrderId,
                    description: description,
                    status: status,
                    dateOpened: dateOpened,
                    dateClosed: (),
                    tasks: []
                };
                
                WorkOrder addedWorkOrder = check client_asset->/assets/[assetTag]/workorders.post(workOrder);
                io:println("Work Order added successfully:");
                io:println(addedWorkOrder.toJsonString());
            } else if choice == "2" {
                string assetTag = io:readln("Asset Tag: ");
                string workOrderId = io:readln("Work Order ID: ");
                string description = io:readln("Description: ");
                string status = io:readln("New Status (OPEN/IN_PROGRESS/CLOSED): ");
                string dateOpened = io:readln("Date Opened (YYYY-MM-DD): ");
                string dateClosed = io:readln("Date Closed (YYYY-MM-DD or leave empty): ");
                
                WorkOrder workOrder = {
                    workOrderId: workOrderId,
                    description: description,
                    status: status,
                    dateOpened: dateOpened,
                    dateClosed: dateClosed == "" ? () : dateClosed,
                    tasks: []
                };
                
                WorkOrder updatedWorkOrder = check client_asset->/assets/[assetTag]/workorders/[workOrderId].put(workOrder);
                io:println("Work Order updated successfully:");
                io:println(updatedWorkOrder.toJsonString());
            } else if choice == "3" {
                string assetTag = io:readln("Asset Tag: ");
                string workOrderId = io:readln("Work Order ID: ");
                string taskId = io:readln("Task ID: ");
                string description = io:readln("Task Description: ");
                string status = io:readln("Task Status (PENDING/IN_PROGRESS/COMPLETED): ");
                
                Task task = {
                    taskId: taskId,
                    description: description,
                    status: status
                };
                
                Task addedTask = check client_asset->/assets/[assetTag]/workorders/[workOrderId]/tasks.post(task);
                io:println("Task added successfully:");
                io:println(addedTask.toJsonString());
            }
        }
        _ => {
            io:println("Invalid option. Please choose a number between 1-11.");
        }
    }
}
