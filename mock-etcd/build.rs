fn main() {
    // grpcio depends on cmake, g++ and protoc,
    // run the following command to install:
    // `sudo apt install cmake g++ libprotobuf-dev protobuf-compiler`
    protoc_grpcio::compile_grpc_protos(
        &[
            "proto/auth.proto",
            "proto/kv.proto",
            "proto/rpc.proto",
            "proto/lock.proto",
        ], // inputs
        &[".."], // includes
        "src",   // output
        None,    // customizations
    )
    .unwrap_or_else(|_| panic!("Failed to compile gRPC definitions!"));
}
