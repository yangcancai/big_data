#!/bin/bash
build(){
    mkdir -p ./priv
    cargo build --manifest-path=crates/big_data/Cargo.toml --release
    sh -c "cp $(cat crates/big_data/libpath) ./priv/libbig_data.so "
}
test(){
    cargo test --manifest-path=crates/big_data/Cargo.toml 
}
clippy(){
    cargo clippy --manifest-path=crates/big_data/Cargo.toml 
}
help(){
    echo "sh build_crates.sh <command> :"
    echo "build              - do cargo build and cp libpath to priv"
    echo "test               - do cargo test"
    echo "clippy             - do cargo clippy to improve your rust code"
    echo "bench              - do cargo bench"
    echo "help               - help to use command"
}
bench(){
    cargo bench --manifest-path=crates/big_data/Cargo.toml 
}
case $1 in
bench) bench;;
build) build;;
test) test;;
clippy) clippy;;
*) help;;
esac