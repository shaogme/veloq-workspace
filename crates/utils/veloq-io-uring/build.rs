use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_OS");

    let target_os = env::var("CARGO_CFG_TARGET_OS").expect("Cargo must provide target OS");
    if target_os != "linux" && target_os != "android" {
        panic!("veloq-io-uring is only supported on Linux and Android targets");
    }

    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").expect("Cargo must provide target arch");
    if !matches!(
        target_arch.as_str(),
        "x86_64" | "aarch64" | "riscv64" | "loongarch64" | "powerpc64"
    ) {
        panic!("veloq-io-uring ABI is unsupported on target architecture {target_arch}");
    }
}
