fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/phalanx.proto");

    tonic_build::compile_protos("proto/phalanx.proto")?;

    Ok(())
}
