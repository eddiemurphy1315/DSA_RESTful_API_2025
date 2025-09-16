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
         
        "6" => {
            string faculty = io:readln("Faculty: ");
            Asset[] assets = check client_asset->/assets/faculty/[faculty];
            io:println("Assets in faculty: " + faculty);
            foreach Asset asset in assets {
                io:println(asset.toJsonString());
            }
        }
        
        _ => {
            io:println("Invalid option. Please choose a number between 1-11.");
        }
    }
}