{ sources ? import ./npins
, system ? builtins.currentSystem
, pkgs ? import sources.nixpkgs { inherit system; config.allowUnfree = true; }
}:
let
  deps = import ./deps.nix { inherit pkgs; };

  # 将 entrypoint 脚本打包
  # 这样它会被放入 nix store，并且其 bin 目录会合并到 image 的 /bin
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

  # 提取环境变量逻辑
  rustSrc = "${pkgs.rustPlatform.rustLibSrc}";
  pkgConfigPath = "${pkgs.openssl.dev}/lib/pkgconfig";
  nixLdLibPath = pkgs.lib.makeLibraryPath deps.runtimeLibs;
  nixLd = pkgs.lib.fileContents "${pkgs.stdenv.cc}/nix-support/dynamic-linker";

in
pkgs.dockerTools.buildLayeredImage {
  name = "veloq-dev";
  tag = "latest";
  
  # 启用 Nix 数据库支持
  includeNixDB = true;

  contents = deps.all ++ [
    # 放入 entrypoint 脚本
    entrypoint

    # 基础配置包
    pkgs.iana-etc
    pkgs.dockerTools.caCertificates
    
    # 配置文件 (非敏感)
    passwd
    group
    sshdConfig
    pamSshd
    nsswitchConf
  ];

  # 使用 extraCommands 进行目录创建和敏感文件生成
  # 这里的命令在构建镜像层时以 Fakeroot 环境执行
  extraCommands = ''
    # 1. 基础系统目录
    mkdir -p tmp
    chmod 1777 tmp
    
    mkdir -p root/.ssh
    chmod 700 root/.ssh
    
    mkdir -p var/run/sshd var/empty/sshd
    
    # 2. 生成 /etc/shadow (避免 store 文件世界可读的问题)
    # root 密码留空 (::)，配合 PermitEmptyPasswords yes 使用
    cat > etc/shadow <<EOF
    root::19733:0:99999:7:::
    sshd:*:19733:0:99999:7:::
    EOF
    chmod 600 etc/shadow

    # 3. 确保其他 PAM 配置存在
    cp etc/pam.d/sshd etc/pam.d/other
    
    # 4. FHS 兼容性 (VS Code Server 等需要)
    mkdir -p lib64 usr/lib64 usr/lib
    ln -sf ${pkgs.nix-ld}/lib/ld-linux-x86-64.so.2 lib64/ld-linux-x86-64.so.2
    
    # libstdc++ 兼容
    ln -sf ${pkgs.stdenv.cc.cc.lib}/lib/libstdc++.so.6 usr/lib/libstdc++.so.6
    ln -sf ${pkgs.stdenv.cc.cc.lib}/lib/libstdc++.so.6 usr/lib64/libstdc++.so.6
    
    # bin/bash 软链接 (确保 /bin/bash 存在)
    ln -sf ${pkgs.bashInteractive}/bin/bash bin/bash

    # 兼容性: 确保 /usr/bin/env 存在 (entrypoint 使用 #!/usr/bin/env bash)
    mkdir -p usr/bin
    ln -sf ${pkgs.coreutils}/bin/env usr/bin/env
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
