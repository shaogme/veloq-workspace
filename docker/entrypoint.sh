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

# 2. System Configuration (Unlock Read-Only Files)
# Decouple system files from Nix store to allow modification
for file in /etc/passwd /etc/shadow /etc/group; do
    if [ -L "$file" ] || [ ! -w "$file" ]; then
        cp "$file" "${file}.tmp" && rm -f "$file" && mv "${file}.tmp" "$file"
    fi
done
chmod 644 /etc/passwd /etc/group
chmod 600 /etc/shadow

# 3. Setup Root Access (Ensure passwordless/root access works)
# If root has a locked password ("!"), clear it to allow "PermitEmptyPasswords yes" to work
sed -i "s|^root:[^:]*|root:|" /etc/shadow

# 4. Setup Authorized Keys
# Installs the host's public key for seamless SSH access if mounted
if [ -f "/tmp/id_ed25519.pub" ]; then
    mkdir -p /root/.ssh
    cp /tmp/id_ed25519.pub /root/.ssh/authorized_keys
    chmod 700 /root/.ssh
    chmod 600 /root/.ssh/authorized_keys
fi

# 5. Export Environment for SSH Sessions
# Captures current environment (Nix paths) and saves it to ~/.ssh/environment.
env | grep -E "^(PATH|NIX_|CARGO_|RUST_|PKG_CONFIG|LD_)" > /root/.ssh/environment || true

# 6. Prepare Runtime Directories
mkdir -p /run/sshd

# 7. Execute Command
if [ $# -gt 0 ]; then
    if command -v "$1" >/dev/null 2>&1; then
        exec "$@"
    else
        exec bash -c "$*"
    fi
fi

# Default: Start SSH Server
echo "Starting SSH server..."
exec $(which sshd) -D -e
