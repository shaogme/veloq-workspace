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
    glibc
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
    bashInteractive # Included here and linked in image.nix
    
    # Network & Utils
    curl
    git
    openssh
    iproute2
    net-tools
    procps
    tcpdump
    vim
    shadow # For user management utilities if needed

    # Debugging
    gdb
    lldb

    # Rust Ecosystem
    cargo
    rustc
    rust-analyzer
    clippy
    rustfmt
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
