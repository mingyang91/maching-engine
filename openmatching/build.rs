extern crate prost_build;

fn main() {
    prost_build::compile_protos(
        &[
            "src/protos/key.proto",
            "src/protos/order.proto",
            "src/protos/command.proto",
        ],
        &["src/protos"],
    )
    .unwrap();
}
