import ballerina/io;
import ballerina/http;
import ballerina/test;

http:Client testClient = check new ("http://localhost:9090/asset_management");

// Test data
map<json> testAsset = {
    "assetTag": "TEST-001",
    "name": "Test 3D Printer",
    "faculty": "Computing & Informatics",
    "department": "Software Engineering",
    "status": "ACTIVE",
    "acquiredDate": "2024-01-15",
    "components": {},
    "schedules": {},
    "workOrders": {}
};

map<json> testComponent = {
    "componentId": "COMP-001",
    "name": "Print Head",
    "description": "Main printing component"
};

map<json> testSchedule = {
    "scheduleId": "SCH-001",
    "frequency": "QUARTERLY",
    "nextDueDate": "2024-12-31",
    "description": "Regular maintenance check"
};

// Before Suite Function
@test:BeforeSuite
function beforeSuiteFunc() {
    io:println("Starting Asset Management API Tests!");
}

// Test: Create Asset
@test:Config {}
function testCreateAsset() returns error? {
    json response = check testClient->post("/assets", testAsset);
    test:assertEquals(response.assetTag, "TEST-001");
    test:assertEquals(response.name, "Test 3D Printer");
    io:println(" Test Create Asset - Passed");
}

// Test: Get All Assets
@test:Config {dependsOn: [testCreateAsset]}
function testGetAllAssets() returns error? {
    json response = check testClient->get("/assets");
    test:assertTrue(response is json[], "Response should be an array");
    io:println(" Test Get All Assets - Passed");
}

// Test: Get Specific Asset
@test:Config {dependsOn: [testCreateAsset]}
function testGetSpecificAsset() returns error? {
    json response = check testClient->get("/assets/TEST-001");
    test:assertEquals(response.assetTag, "TEST-001");
    test:assertEquals(response.name, "Test 3D Printer");
    io:println(" Test Get Specific Asset - Passed");
}

// Test: Update Asset
@test:Config {dependsOn: [testCreateAsset]}
function testUpdateAsset() returns error? {
    map<json> updatedAsset = testAsset.clone();
    updatedAsset["name"] = "Updated 3D Printer";
    updatedAsset["status"] = "UNDER_REPAIR";
    
    json response = check testClient->put("/assets/TEST-001", updatedAsset);
    test:assertEquals(response.name, "Updated 3D Printer");
    test:assertEquals(response.status, "UNDER_REPAIR");
    io:println(" Test Update Asset - Passed");
}

// Test: Get Assets by Faculty
@test:Config {dependsOn: [testCreateAsset]}
function testGetAssetsByFaculty() returns error? {
    json response = check testClient->get("/assets/faculty/Computing%20%26%20Informatics");
    test:assertTrue(response is json[], "Response should be an array");
    io:println(" Test Get Assets by Faculty - Passed");
}

// Test: Add Component to Asset
@test:Config {dependsOn: [testCreateAsset]}
function testAddComponent() returns error? {
    json response = check testClient->post("/assets/TEST-001/components", testComponent);
    test:assertEquals(response.componentId, "COMP-001");
    test:assertEquals(response.name, "Print Head");
    io:println(" Test Add Component - Passed");
}

// Test: Add Schedule to Asset
@test:Config {dependsOn: [testCreateAsset]}
function testAddSchedule() returns error? {
    json response = check testClient->post("/assets/TEST-001/schedules", testSchedule);
    test:assertEquals(response.scheduleId, "SCH-001");
    test:assertEquals(response.frequency, "QUARTERLY");
    io:println(" Test Add Schedule - Passed");
}

// Test: Get Overdue Assets
@test:Config {dependsOn: [testAddSchedule]}
function testGetOverdueAssets() returns error? {
    // This test depends on the current date and schedule dates
    json response = check testClient->get("/assets/overdue");
    test:assertTrue(response is json[], "Response should be an array");
    io:println(" Test Get Overdue Assets - Passed");
}

// Test: Add Work Order
@test:Config {dependsOn: [testCreateAsset]}
function testAddWorkOrder() returns error? {
    map<json> workOrder = {
        "workOrderId": "WO-001",
        "description": "Fix printing issue",
        "status": "OPEN",
        "dateOpened": "2024-03-15",
        "dateClosed": null,
        "tasks": []
    };
    
    json response = check testClient->post("/assets/TEST-001/workorders", workOrder);
    test:assertEquals(response.workOrderId, "WO-001");
    test:assertEquals(response.status, "OPEN");
    io:println(" Test Add Work Order - Passed");
}

// Test: Add Task to Work Order
@test:Config {dependsOn: [testAddWorkOrder]}
function testAddTaskToWorkOrder() returns error? {
    map<json> task = {
        "taskId": "TASK-001",
        "description": "Replace print head",
        "status": "PENDING"
    };
    
    json response = check testClient->post("/assets/TEST-001/workorders/WO-001/tasks", task);
    test:assertEquals(response.taskId, "TASK-001");
    test:assertEquals(response.status, "PENDING");
    io:println(" Test Add Task to Work Order - Passed");
}

// Negative Test: Get Non-existent Asset
@test:Config {}
function testGetNonExistentAsset() returns error? {
    http:Response response = check testClient->get("/assets/NON-EXISTENT");
    test:assertEquals(response.statusCode, 500);
    io:println(" Test Get Non-existent Asset - Passed");
}

// Negative Test: Create Duplicate Asset
@test:Config {dependsOn: [testCreateAsset]}
function testCreateDuplicateAsset() returns error? {
    http:Response response = check testClient->post("/assets", testAsset);
    test:assertEquals(response.statusCode, 500);
    json errorPayload = check response.getJsonPayload();
    test:assertTrue(errorPayload.toString().includes("already exists"));
    io:println(" Test Create Duplicate Asset - Passed");
}

// Test: Remove Component
@test:Config {dependsOn: [testAddComponent]}
function testRemoveComponent() returns error? {
    json response = check testClient->delete("/assets/TEST-001/components/COMP-001");
    test:assertEquals(response.componentId, "COMP-001");
    io:println(" Test Remove Component - Passed");
}

// Test: Remove Schedule
@test:Config {dependsOn: [testAddSchedule]}
function testRemoveSchedule() returns error? {
    json response = check testClient->delete("/assets/TEST-001/schedules/SCH-001");
    test:assertEquals(response.scheduleId, "SCH-001");
    io:println(" Test Remove Schedule - Passed");
}

// Test: Delete Asset (should be last test)
@test:Config {dependsOn: [testUpdateAsset, testGetAssetsByFaculty, testRemoveComponent, testRemoveSchedule]}
function testDeleteAsset() returns error? {
    json response = check testClient->delete("/assets/TEST-001");
    test:assertEquals(response.assetTag, "TEST-001");
    io:println(" Test Delete Asset - Passed");
}

// After Suite Function
@test:AfterSuite
function afterSuiteFunc() {
    io:println("Asset Management API Tests Completed!");
}