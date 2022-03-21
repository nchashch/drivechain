// build.rs

fn main() {
    cxx_build::bridge("src/bridge.rs").compile("drivechain");
}
