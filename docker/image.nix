{ sources ? import ./npins
, system ? builtins.currentSystem
, pkgs ? import sources.nixpkgs { inherit system; config.allowUnfree = true; }
, imageName
}:
let
  deps = import ./deps.nix { inherit pkgs; };

  # Bundle the entrypoint script
  # It will be placed in the nix store, and its bin directory will be merged into /bin
  entrypoint = pkgs.writeScriptBin "entrypoint.sh" (builtins.readFile ./entrypoint.sh);

  passwd = pkgs.writeTextDir "etc/passwd" ''
    root:x:0:0:System Administrator:/root:/bin/bash
    sshd:x:74:74:Privilege-separated SSH:/var/empty/sshd:/sbin/nologin
  '';

  group = pkgs.writeTextDir "etc/group" ''
    root:x:0:
    sshd:x:74:
  '';

  sshdConfig = pkgs.writeTextDir "etc/ssh/sshd_config" ''
    PermitRootLogin yes
    PasswordAuthentication yes
    PubkeyAuthentication yes
    UsePAM yes
    Port 22
    HostKey /etc/ssh/ssh_host_rsa_key
    HostKey /etc/ssh/ssh_host_ed25519_key
    Subsystem sftp internal-sftp
    PermitUserEnvironment yes
    PermitEmptyPasswords yes
  '';

  pamSshd = pkgs.writeTextDir "etc/pam.d/sshd" ''
    auth       sufficient   pam_permit.so
    account    sufficient   pam_permit.so
    password   sufficient   pam_permit.so
    session    sufficient   pam_permit.so
  '';

  nsswitchConf = pkgs.writeTextDir "etc/nsswitch.conf" ''
    passwd:    files
    group:     files
    shadow:    files
    hosts:     files dns
  '';

  # Extract environment logic
  rustSrc = "${pkgs.rustPlatform.rustLibSrc}";
  pkgConfigPath = "${pkgs.openssl.dev}/lib/pkgconfig";
  nixLdLibPath = pkgs.lib.makeLibraryPath deps.runtimeLibs;
  nixLd = pkgs.lib.fileContents "${pkgs.stdenv.cc}/nix-support/dynamic-linker";

  # Wrapper script to enforce environment variables in SSH sessions
  # This solves the issue where SSH wipes LD_LIBRARY_PATH
  bashWrapper = pkgs.writeScript "bash-wrapper" ''
    #!${pkgs.bashInteractive}/bin/bash
    export NIX_LD_LIBRARY_PATH="${nixLdLibPath}:/usr/lib:/usr/lib64"
    export NIX_LD="${nixLd}"
    export LD_LIBRARY_PATH="${nixLdLibPath}:/usr/lib:/usr/lib64"
    export RUST_SRC_PATH="${rustSrc}"
    export PKG_CONFIG_PATH="${pkgConfigPath}"
    export PATH=$PATH:/usr/bin:/bin
    exec ${pkgs.bashInteractive}/bin/bash "$@"
  '';

in
pkgs.dockerTools.buildLayeredImage {
  name = imageName;
  tag = "latest";
  
  # Enable Nix database support
  includeNixDB = true;

  contents = deps.all ++ [
    # Include entrypoint script
    entrypoint

    # Basic configuration packages
    pkgs.iana-etc
    pkgs.dockerTools.caCertificates
    
    # Configuration files (non-sensitive)
    passwd
    group
    sshdConfig
    pamSshd
    nsswitchConf
  ];

  # Use extraCommands for directory creation and sensitive file generation
  # These commands run in a Fakeroot environment during layer construction
  extraCommands = ''
    # 1. Base System Directories
    mkdir -p tmp
    chmod 1777 tmp
    
    mkdir -p root/.ssh
    chmod 700 root/.ssh
    
    mkdir -p var/run/sshd var/empty/sshd
    mkdir -p var/lock
    chmod 1777 var/lock
    mkdir -p var/tmp
    chmod 1777 var/tmp
    
    # 2. Generate /etc/shadow
    # We generate this here to avoid world-readable permissions in the Nix store.
    # Root password is empty (::) to work with PermitEmptyPasswords yes.
    cat > etc/shadow <<EOF
    root::19733:0:99999:7:::
    sshd:*:19733:0:99999:7:::
    EOF
    chmod 600 etc/shadow

    # 3. Ensure PAM Configuration
    # Copy sshd PAM config to 'other' as a fallback
    cp etc/pam.d/sshd etc/pam.d/other
    
    # 4. FHS Compatibility (Required for VS Code Server, etc.)
    # Create standard directory structure
    mkdir -p lib64 usr/lib64 usr/lib usr/bin usr/lib/x86_64-linux-gnu

    # Link Dynamic Linker (ld-linux)
    ln -sf ${pkgs.glibc}/lib/ld-linux-x86-64.so.2 lib64/ld-linux-x86-64.so.2
    
    # Link Core Libraries (libstdc++, libgcc_s)
    # We copy the actual files (cp -L) to ensure they work even if symlinks fail
    # or if applications don't follow Nix store links correctly.
    for lib in libstdc++.so.6 libgcc_s.so.1; do
      # Copy to primary paths
      cp -L ${pkgs.stdenv.cc.cc.lib}/lib/$lib usr/lib/$lib
      cp -L ${pkgs.stdenv.cc.cc.lib}/lib/$lib usr/lib64/$lib
      
      # Symlink for other common paths
      ln -sf /usr/lib/$lib lib64/$lib
      ln -sf /usr/lib/$lib usr/lib/x86_64-linux-gnu/$lib
    done
    
    # 5. Bash Wrapper
    # Replace the default bash symlink with our wrapper script.
    # This ensures LD_LIBRARY_PATH is set for all sessions, including SSH.
    rm -f bin/bash
    cp ${bashWrapper} bin/bash
    chmod +x bin/bash

    # 6. Setup /usr/bin/env
    ln -sf ${pkgs.coreutils}/bin/env usr/bin/env
    
    # 7. Common Tools Symlinks
    # Some tools check specific paths (like VS Code Server checking /usr/bin/ps)
    ln -sf ${pkgs.procps}/bin/pgrep usr/bin/pgrep
    ln -sf ${pkgs.procps}/bin/pkill usr/bin/pkill
    ln -sf ${pkgs.procps}/bin/ps usr/bin/ps
    ln -sf ${pkgs.coreutils}/bin/uname usr/bin/uname
    ln -sf ${pkgs.coreutils}/bin/dirname usr/bin/dirname
    ln -sf ${pkgs.coreutils}/bin/readlink usr/bin/readlink
    ln -sf ${pkgs.coreutils}/bin/wc usr/bin/wc
  '';

  config = {
    Cmd = [ "/bin/entrypoint.sh" ];
    WorkingDir = "/root/workspace";
    
    ExposedPorts = {
      "22/tcp" = {};
    };

    Env = [
      "NIX_SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
      "RUST_SRC_PATH=${rustSrc}"
      "PKG_CONFIG_PATH=${pkgConfigPath}"
      "NIX_LD_LIBRARY_PATH=${nixLdLibPath}:/usr/lib:/usr/lib64"
      "NIX_LD=${nixLd}"
      "LD_LIBRARY_PATH=${nixLdLibPath}:/usr/lib:/usr/lib64"
      "PATH=/bin:/usr/bin:/usr/local/bin"
    ];
  };
}
