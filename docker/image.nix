{ sources ? import ./npins
, system ? builtins.currentSystem
, pkgs ? import sources.nixpkgs { inherit system; config.allowUnfree = true; }
}:
let
  deps = import ./deps.nix { inherit pkgs; };
  
  # 提取环境变量逻辑 (对应 shell.nix)
  rustSrc = "${pkgs.rustPlatform.rustLibSrc}";
  pkgConfigPath = "${pkgs.openssl.dev}/lib/pkgconfig";
  nixLdLibPath = pkgs.lib.makeLibraryPath deps.runtimeLibs;
  nixLd = pkgs.lib.fileContents "${pkgs.stdenv.cc}/nix-support/dynamic-linker";
  
  # /etc/passwd
  etcPasswd = pkgs.writeText "passwd" ''
    root:x:0:0:System Administrator:/root:/bin/bash
    sshd:x:74:74:Privilege-separated SSH:/var/empty/sshd:/sbin/nologin
  '';

  # /etc/group
  etcGroup = pkgs.writeText "group" ''
    root:x:0:
    sshd:x:74:
  '';

  # /etc/shadow
  etcShadow = pkgs.writeText "shadow" ''
    root:*:19733:0:99999:7:::
    sshd:*:19733:0:99999:7:::
  '';

  # /etc/ssh/sshd_config
  sshdConfig = pkgs.writeText "sshd_config" ''
    PermitRootLogin yes
    PasswordAuthentication yes
    PubkeyAuthentication yes
    UsePAM yes
    Port 22
    # HostKeys will be generated at runtime
    HostKey /etc/ssh/ssh_host_rsa_key
    HostKey /etc/ssh/ssh_host_ed25519_key
    Subsystem sftp internal-sftp
    PermitUserEnvironment yes
    PermitEmptyPasswords yes
  '';

  # /etc/pam.d/sshd
  pamSshd = pkgs.writeText "pam-sshd" ''
    auth       sufficient   pam_permit.so
    account    sufficient   pam_permit.so
    password   sufficient   pam_permit.so
    session    sufficient   pam_permit.so
  '';

  # /etc/nsswitch.conf
  nsswitchConf = pkgs.writeText "nsswitch.conf" ''
    passwd:    files
    group:     files
    shadow:    files
    hosts:     files dns
  '';

in
pkgs.dockerTools.buildLayeredImage {
  name = "veloq-dev";
  tag = "latest";

  contents = deps.all ++ [
    pkgs.bashInteractive
    pkgs.coreutils
    pkgs.iana-etc
    pkgs.dockerTools.caCertificates
    pkgs.openssh
  ];

  fakeRootCommands = ''
    # 1. 基础目录结构
    mkdir -p ./tmp ./root/.ssh ./var/run/sshd ./var/empty/sshd ./etc/ssh ./etc/pam.d
    chmod 1777 ./tmp

    # 2. 注入配置文件
    cp ${etcPasswd} ./etc/passwd
    cp ${etcGroup} ./etc/group
    cp ${etcShadow} ./etc/shadow
    cp ${sshdConfig} ./etc/ssh/sshd_config
    cp ${pamSshd} ./etc/pam.d/sshd
    cp ./etc/pam.d/sshd ./etc/pam.d/other
    cp ${nsswitchConf} ./etc/nsswitch.conf

    # 3. FHS 兼容性 (lib64 等)
    mkdir -p ./lib64 ./usr/lib64 ./usr/lib
    ln -s ${pkgs.nix-ld}/lib/ld-linux-x86-64.so.2 ./lib64/ld-linux-x86-64.so.2
    
    # 处理 libstdc++ 链接
    ln -s ${pkgs.stdenv.cc.cc.lib}/lib/libstdc++.so.6 ./usr/lib/libstdc++.so.6
    ln -s ${pkgs.stdenv.cc.cc.lib}/lib/libstdc++.so.6 ./usr/lib64/libstdc++.so.6
    
    # Symlink standard tools
    ln -sf ${pkgs.bashInteractive}/bin/bash ./bin/bash
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
      "NIX_LD_LIBRARY_PATH=${nixLdLibPath}"
      "NIX_LD=${nixLd}"
      "LD_LIBRARY_PATH=${nixLdLibPath}"
      "PATH=/bin:/usr/bin:/usr/local/bin"
    ];
  };
}
