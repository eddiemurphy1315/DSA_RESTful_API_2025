import ballerina/grpc;
import ballerina/log;
import ballerina/uuid;

// In-memory storage
map<Car> cars = {};
map<User> users = {};
map<CartItem[]> customerCarts = {};
map<Reservation> reservations = {};

listener grpc:Listener ep = new (9090);

@grpc:Descriptor {value: USERS_DESC}
service "CarRentalService" on ep {

     //Murphy
    remote function add_car(AddCarRequest value) returns AddCarResponse|error {
        Car car = value.car;
        
        // Validate required fields
        if car.plate == "" || car.make == "" || car.model == "" {
            return error("Car plate, make, and model are required");
        }
        
        // Check if car already exists
        if cars.hasKey(car.plate) {
            return error("Car with plate " + car.plate + " already exists");
        }
        
        // Add car to inventory
        cars[car.plate] = car;
        
        log:printInfo("Car added successfully: " + car.plate);
        
        return {plate: car.plate};
    }

        //Henry------------------------------
        //update car details 
    remote function update_car(UpdateCarRequest value) returns UpdateCarResponse|error {
    string plate = value.plate;
    Car updatedCar = value.updated_car;

    if plate == "" {
        return error("Car plate is required for update");
    }
    if updatedCar.plate == "" || updatedCar.make == "" || updatedCar.model == "" {
        return error("Updated car must have plate, make, and model");
    }
    if !cars.hasKey(plate) {
        return error("Car with plate " + plate + " not found");
    }
    if plate != updatedCar.plate {
        return error("Plate in request (" + plate + ") does not match updated car's plate (" + updatedCar.plate + ")");
    }
    if updatedCar.year < 1900 || updatedCar.year > 2025 {
        return error("Invalid car year. Must be between 1900 and 2025");
    }
    if updatedCar.daily_price < 0.0 {
        return error("Daily price must be non-negative");
    }
    if updatedCar.mileage < 0 {
        return error("Mileage must be non-negative");
    }
    if updatedCar.status != "AVAILABLE" && updatedCar.status != "UNAVAILABLE" && updatedCar.status != "RENTED" {
        return error("Invalid car status. Must be AVAILABLE, UNAVAILABLE, or RENTED");
    }

    // Update in system
    cars[plate] = updatedCar;
    log:printInfo("Car updated successfully: " + plate);

    //  Only `message` exists in UpdateCarResponse
    return { message: "Car updated successfully: " + plate };
}


    remote function remove_car(RemoveCarRequest value) returns RemoveCarResponse|error {
        string plate = value.plate;
        
        // Check if car exists
        if !cars.hasKey(plate) {
            return error("Car with plate " + plate + " not found");
        }
        
        // Remove car from inventory
        _ = cars.remove(plate);
        
        // Return updated car list
        Car[] carList = [];
        foreach Car car in cars {
            carList.push(car);
        }
        
        log:printInfo("Car removed successfully: " + plate);
        
        return {cars: carList};
    }

    remote function search_car(SearchCarRequest value) returns SearchCarResponse|error {
        //Mutombo
    }

      //Murphy
    remote function add_to_cart(AddToCartRequest value) returns AddToCartResponse|error {
        string customerId = value.customer_id;
        string plate = value.plate;
        string startDate = value.start_date;
        string endDate = value.end_date;
        
        // Validate dates
        if startDate == "" || endDate == "" {
            return error("Start date and end date are required");
        }
        
        // Check if car exists and is available
        if !cars.hasKey(plate) {
            return error("Car with plate " + plate + " not found");
        }
        
        Car car = cars.get(plate);
        if car.status != AVAILABLE {
            return error("Car is not available for rental");
        }
        
        // Create cart item
        CartItem cartItem = {
            plate: plate,
            start_date: startDate,
            end_date: endDate
        };
        
        // Add to customer's cart
        if customerCarts.hasKey(customerId) {
            CartItem[] existingCart = customerCarts.get(customerId);
            existingCart.push(cartItem);
            customerCarts[customerId] = existingCart;
        } else {
            customerCarts[customerId] = [cartItem];
        }
        
        log:printInfo("Item added to cart for customer: " + customerId);
        
        return {message: "Item added to cart successfully"};
    }

    remote function place_reservation(PlaceReservationRequest value) returns PlaceReservationResponse|error {
        //Mutombo
    }

     
    remote function create_users(stream<CreateUserRequest, grpc:Error?> clientStream) returns CreateUsersResponse|error {
        //Patrick
    }

     //Murphy
    remote function list_available_cars(ListAvailableCarsRequest value) returns stream<Car, error?>|error {
        string filter = value.filter;
        
        // Create array of available cars
        Car[] availableCars = [];
        
        foreach Car car in cars {
            if car.status == AVAILABLE {
                // Apply filter if provided
                if filter == "" {
                    availableCars.push(car);
                } else {
                    // Simple filter matching make, model, or year
                    if car.make.toLowerAscii().includes(filter.toLowerAscii()) ||
                       car.model.toLowerAscii().includes(filter.toLowerAscii()) ||
                       car.year.toString().includes(filter) {
                        availableCars.push(car);
                    }
                }
            }
        }
        
        return availableCars.toStream();
    }
}
