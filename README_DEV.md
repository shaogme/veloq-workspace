# Docker Development Environment

This directory contains the configuration for a Docker-based Linux development environment, powered by **Nix** for reproducible builds.

## 1. Prerequisites

- Docker Desktop installed and running.
- **Windows Users**: You can run this setup directly from PowerShell or WSL2.


## 2. Start the Environment

### Option A: Development Mode (Recommended)
Use this for active development. Your local source code is mounted into the container, allowing hot-reloading/live-editing.

```bash
docker-compose up -d --build dev
```

### Option B: Standalone Mode
Use this to test the self-contained image. The source code is copied into the image at build time and is isolated from your local file system changes.

```bash
docker-compose up -d --build standalone
```

**Note**: Both modes use the same ports (SSH 2222, App 8080+), so stop one before starting the other.

This will build the image and start the container (`veloq-dev` or `veloq-standalone`).

## 3. Connecting via SSH

You can connect to the container using SSH:

```bash
ssh root@localhost -p 2222
# Password: root
```

Alternatively, add the following to your `~/.ssh/config` file for easier access:

```ssh
Host veloq-dev
    HostName localhost
    Port 2222
    User root
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    IdentityFile ~/.ssh/id_ed25519
```

Then you can simply run: `ssh veloq-dev`

## 4. Running Commands Directly

You can execute cargo commands directly inside the container without SSH:

```bash
# Run cargo check
docker-compose run --rm --build standalone cargo check

# Run tests
docker-compose run --rm --build standalone cargo test

# Open a shell
docker-compose run --rm dev bash
```

## 5. Connecting via VSCode (Remote - SSH)

1. Open VSCode.
2. Press `F1` (or `Ctrl+Shift+P`) and run **Remote-SSH: Connect to Host...**.
3. Enter: `ssh root@localhost -p 2222`.
4. Enter password `root` when prompted.
5. Once connected, open the `/root/workspace` folder.

## 6. Performance Benchmarking

For consistent results, run benchmarks in `standalone` mode to avoid filesystem bridging overhead:

```bash
docker-compose run --rm standalone cargo bench
```

## 7. Notes

- **Source Code**: The current directory is mounted to `/root/workspace` in the container. Changes propagate instantly.
- **Port Mapping**:
    - `2222` -> `22` (SSH)
    - `8080`, `8081`, `9000` are mapped for your application availability.
- **Tools**: Installed tools include `rust`, `cargo`, `gdb`, `lldb`, `iproute2`, `tcpdump`.
