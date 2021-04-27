#!/bin/bash
mkdir -p ./priv
cargo build --manifest-path=crates/big_data/Cargo.toml --release
sh -c "cp $(cat crates/big_data/libpath) ./priv/libbig_data.so "