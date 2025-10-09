use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = vec![
        "protos/axelar/reward/v1beta1/tx.proto",
        "protos/axelar/permission/exported/v1beta1/types.proto",
        "protos/axelar/tss/v1beta1/tx.proto",
    ];

    let proto_includes = vec![
        "protos",
    ];

    std::fs::create_dir_all("src/generated")?;

    prost_build::Config::new()
        .out_dir(PathBuf::from("src/generated"))
        .compile_protos(&proto_files, &proto_includes)?;

    Ok(())
}
