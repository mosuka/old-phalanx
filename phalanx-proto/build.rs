fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/index.proto");

    tonic_build::compile_protos("proto/index.proto")?;
    Ok(())
}
