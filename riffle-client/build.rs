fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/manager.proto");
    println!("cargo:rerun-if-changed=../riffle-server/src/grpc/protobuf/uniffle.proto");
    tonic_prost_build::configure()
        .extern_path(".rss.common", "::riffle_server::grpc::protobuf::uniffle")
        .compile_protos(&["proto/manager.proto"], &["proto", "../riffle-server/src"])?;
    Ok(())
}
