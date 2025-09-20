import ballerina/grpc;

listener grpc:Listener ep = new (9090);

@grpc:Descriptor {value: USERS_DESC}
service "CarRentalService" on ep {

    remote function add_car(AddCarRequest value) returns AddCarResponse|error {
        //Murphy
    }

    remote function update_car(UpdateCarRequest value) returns UpdateCarResponse|error {
        //Henry
    }

    remote function remove_car(RemoveCarRequest value) returns RemoveCarResponse|error {
        //Mbanga
    }

    remote function search_car(SearchCarRequest value) returns SearchCarResponse|error {
        //Mutombo
    }

    remote function add_to_cart(AddToCartRequest value) returns AddToCartResponse|error {
        //Patrick
    }

    remote function place_reservation(PlaceReservationRequest value) returns PlaceReservationResponse|error {
        //Mutombo
    }

    remote function create_users(stream<CreateUserRequest, grpc:Error?> clientStream) returns CreateUsersResponse|error {
        //Patrick
    }

    remote function list_available_cars(ListAvailableCarsRequest value) returns stream<Car, error?>|error {
        //Mbanga
    }
}