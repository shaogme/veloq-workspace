{ pkgs }:
let
  # 1. System Compatibility Libraries (nix-ld)
  # Commonly required by VS Code Server, Copilot, etc.
  runtimeLibs = with pkgs; [
    stdenv.cc.cc.lib
    zlib
    openssl
    icu
    libsecret
    glib
    libkrb5
    util-linux
  ];

  # 2. Development Tools
  devTools = with pkgs; [
    # Core
    gcc
    glibc
    glibc.bin # Contains ldd, required for version checks
    coreutils
    findutils
    gnugrep
    gnused
    gawk
    gnutar
    gzip
    wget
    which
    xz
    cacert
    bashInteractive

    # Network & Utils
    curl
    git
    openssh
    iproute2
    net-tools
    procps
    tcpdump
    vim
    shadow

    # Debugging
    gdb
    lldb

    # Rust Ecosystem
    cargo
    rustc
    rust-analyzer
    clippy
    rustfmt
    cargo-nextest
    pkg-config
    openssl.dev

    # Nix Utilities
    nix
    nix-ld
    direnv
    nix-direnv
  ];
in
{
  inherit runtimeLibs devTools;
  all = devTools ++ runtimeLibs;
}
