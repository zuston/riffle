fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/uniffle.proto");

    let mut config = prost_build::Config::new();
    config.bytes(&["."]);

    tonic_prost_build::configure()
        .build_server(true)
        .compile_with_config(config, &["proto/uniffle.proto"], &["proto"])?;
    Ok(())
}
