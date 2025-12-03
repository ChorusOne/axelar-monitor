use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = vec![
        "protos/axelar/reward/v1beta1/tx.proto",
        "protos/axelar/permission/exported/v1beta1/types.proto",
        "protos/axelar/tss/v1beta1/tx.proto",
        "protos/axelar/evm/v1beta1/tx.proto",
        "protos/axelar/evm/v1beta1/types.proto",
        "protos/axelar/evm/v1beta1/events.proto",
        "protos/axelar/vote/v1beta1/tx.proto",
    ];

    let proto_includes = vec!["protos"];

    std::fs::create_dir_all("src/generated")?;

    let mut config = prost_build::Config::new();
    config.out_dir(PathBuf::from("src/generated"));

    // Use cosmos types from cosmos-sdk-proto crate instead of generating them
    config.extern_path(
        ".cosmos.base.v1beta1",
        "::cosmos_sdk_proto::cosmos::base::v1beta1",
    );
    config.extern_path(
        ".cosmos.base.abci.v1beta1",
        "::cosmos_sdk_proto::cosmos::base::abci::v1beta1",
    );

    config.compile_protos(&proto_files, &proto_includes)?;

    Ok(())
}
