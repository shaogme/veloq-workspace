#!/usr/bin/env bash
set -e

# ==========================================
# NixOS Development Container Entrypoint
# ==========================================

# 1. Initialize SSH Host Keys
# Only generate if they don't exist
if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then
    ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -t rsa >/dev/null 2>&1
fi
if [ ! -f /etc/ssh/ssh_host_ed25519_key ]; then
    ssh-keygen -f /etc/ssh/ssh_host_ed25519_key -N '' -t ed25519 >/dev/null 2>&1
fi

# 2. Fix Permissions for Config Files (Symlink Handling)
# Files from 'contents' in image.nix are usually symlinks to the Nix Store (read-only).
# If we need them to be writable (e.g., for user management), we must copy them.
for file in /etc/passwd /etc/group; do
    if [ -L "$file" ] || [ ! -w "$file" ]; then
        cp --remove-destination "$(readlink -f $file)" "$file"
        chmod 644 "$file"
    fi
done

# Shadow is created via extraCommands so it should be a file, but ensuring permissions is safe.
if [ -L "/etc/shadow" ]; then
    cp --remove-destination "$(readlink -f /etc/shadow)" /etc/shadow
fi
chmod 600 /etc/shadow

# 3. Setup Authorized Keys
if [ -f "/tmp/id_ed25519.pub" ]; then
    mkdir -p /root/.ssh
    if [ ! -f /root/.ssh/authorized_keys ]; then
        cp /tmp/id_ed25519.pub /root/.ssh/authorized_keys
        chmod 700 /root/.ssh
        chmod 600 /root/.ssh/authorized_keys
    fi
fi

# 3.1 Ensure Metadata Directories
# In case these are mounted as tmpfs or valid directories are missing
mkdir -p /var/lock /var/tmp
chmod 1777 /var/lock /var/tmp

# 4. Export Environment for SSH Sessions
env | grep -E "^(PATH|NIX_|CARGO_|RUST_|PKG_CONFIG|LD_)" > /root/.ssh/environment || true
chmod 600 /root/.ssh/environment

# 5. Execute Command or Start SSHD
if [ $# -gt 0 ]; then
    exec "$@"
else
    echo "Starting SSH server..."
    exec $(which sshd) -D -e
fi
