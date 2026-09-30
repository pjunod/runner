#!/usr/bin/env python3
"""Exercise both Compose ownership helpers using NZBD_TEST_IMAGE (default nzbd).

Uses disposable Docker volumes to test Linux ownership even on Docker Desktop.
No deployment directories or running Runner containers are touched.
"""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid


ROOT = Path(__file__).resolve().parents[1]
IMAGE = os.environ.get("NZBD_TEST_IMAGE", "nzbd:latest")


def docker(*args, check=True):
    return subprocess.run(
        ["docker", *args], check=check, text=True, capture_output=True, timeout=120
    )


def check_recipe(recipe):
    config = json.loads(docker("compose", "-f", str(ROOT / recipe), "config", "--format", "json").stdout)
    daemon = config["services"]["nzbd"]
    helper = config["services"]["nzbd-init"]
    assert daemon["volumes"] == helper["volumes"], "init must use the daemon's mounts"
    project = "nzbd-permissions-" + uuid.uuid4().hex[:12]
    mounts = [
        {"type": "volume", "source": "config", "target": "/etc/nzbd"},
        {"type": "volume", "source": "data", "target": "/data"},
    ]
    # Keep the shipped initialization command, identity and dependency gate.
    # Replace only deployment paths, image and the daemon's long-running command.
    helper = {k: v for k, v in helper.items() if k not in ("build", "image", "volumes")}
    helper.update(image=IMAGE, volumes=mounts)
    probe = {
        "image": IMAGE,
        "volumes": mounts,
        "depends_on": daemon["depends_on"],
        "entrypoint": ["/bin/sh", "-ec"],
        "command": [
            'test "$(id -u):$(id -g)" = 1000:1000; '
            'test "$(stat -c %u:%g /etc/nzbd)" = 1000:1000; '
            'test "$(stat -c %u:%g /data)" = 1000:1000; '
            'test "$(stat -c %u:%g /data/existing)" = 1234:1234; '
            'echo saved > /etc/nzbd/nzbd.toml; '
            'touch /etc/nzbd/new-config /data/new-state'
        ],
    }
    if "user" in daemon:
        probe["user"] = daemon["user"]
    test_config = {
        "services": {"nzbd-init": helper, "nzbd": probe},
        "volumes": {"config": {}, "data": {}},
    }
    with tempfile.TemporaryDirectory(prefix=project) as tmp:
        path = Path(tmp) / "compose.json"
        path.write_text(json.dumps(test_config))
        compose = ["compose", "-p", project, "-f", str(path)]
        try:
            # Reproduce root-created mounts, plus a private pre-seeded config.
            docker(*compose, "run", "--rm", "--no-deps", "--user", "0:0",
                   "--entrypoint", "/bin/sh", "nzbd", "-ec",
                   "chown 0:0 /etc/nzbd /data; chmod 755 /etc/nzbd /data; "
                   "touch /data/existing; chown 1234:1234 /data/existing")
            for seeded in (False, True):
                if seeded:
                    docker(*compose, "run", "--rm", "--no-deps", "--user", "0:0",
                           "--entrypoint", "/bin/sh", "nzbd", "-ec",
                           "echo original > /etc/nzbd/nzbd.toml; "
                           "chown 0:0 /etc/nzbd/nzbd.toml; chmod 600 /etc/nzbd/nzbd.toml")
                docker(*compose, "up", "-d", "--force-recreate", "nzbd")
                container = docker(*compose, "ps", "-a", "-q", "nzbd").stdout.strip()
                assert docker("wait", container).stdout.strip() == "0", docker(*compose, "logs").stdout
                print(f"{recipe}: {'root-owned existing config' if seeded else 'fresh config'} writable as 1000:1000")

            # An ownership failure must keep the daemon from running.
            docker(*compose, "down")
            helper["volumes"] = [dict(mount, read_only=True) for mount in mounts]
            path.write_text(json.dumps(test_config))
            failed = docker(*compose, "up", "-d", "nzbd", check=False)
            assert failed.returncode != 0, "read-only initialization must fail"
            container = docker(*compose, "ps", "-a", "-q", "nzbd").stdout.strip()
            if container:
                state = docker("inspect", "-f", "{{.State.Status}}", container).stdout.strip()
                assert state == "created", f"daemon started after failed initialization: {state}"
            print(f"{recipe}: failed initialization blocks daemon startup")
        finally:
            docker(*compose, "down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    try:
        for recipe in ("examples/docker-compose/docker-compose.yml", "dev/docker-compose.yml"):
            check_recipe(recipe)
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.stderr or error.stdout) from error
