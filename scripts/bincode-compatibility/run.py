"""Independent old/new bytes, encrypted keys, Raft/WAL and snapshot recovery."""

import argparse, hashlib, json, pathlib, queue, shutil, socket, subprocess, sys, threading, time

# Progress lines name old→new directions; never depend on the console code page.
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

p = argparse.ArgumentParser()
p.add_argument("--work-root", required=True)
p.add_argument("--freeze", action="store_true")
p.add_argument("--fixtures-only", action="store_true")
p.add_argument("--profile", choices=["sqlite", "cache-backup"], default="sqlite")
a = p.parse_args()
tag = "" if a.profile == "sqlite" else "-cache-backup"
root = pathlib.Path(__file__).resolve().parents[2]
work = pathlib.Path(a.work_root).resolve()
suffix = ".exe" if __import__("os").name == "nt" else ""
executables = {
    side: work / "bin" / (side + "-probe" + tag + suffix)
    for side in ["legacy", "candidate"]
}
assert all(e.is_file() for e in executables.values())
report = {
    "baseline": "5e44e1acfb590c8657a10cf7983f8c37cb04c654",
    "profile": a.profile,
    "candidate_source": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, encoding="utf-8"
    ).strip(),
    "binary_sha256": {
        s: hashlib.sha256(e.read_bytes()).hexdigest() for s, e in executables.items()
    },
    "checks": [],
}


def checked(name):
    report["checks"].append(name)
    (work / "compatibility-report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("PASS", name, flush=True)


for side, exe in executables.items():
    subprocess.run(
        [str(exe), "fixtures", str(work / (side + "-fixtures"))], check=True, timeout=90
    )
for name in ["hiqlite.json", "wal.json", "peer.json", "keys.bin", "key-errors.json"]:
    old = (work / "legacy-fixtures" / name).read_bytes()
    new = (work / "candidate-fixtures" / name).read_bytes()
    assert old == new, f"old/new byte or error behavior mismatch: {name}"
    checked("independent byte/error equality: " + name)
for side, exe in executables.items():
    for writer in executables:
        subprocess.run(
            [str(exe), "keys-read", str(work / (writer + "-fixtures"))],
            check=True,
            timeout=90,
        )
checked("native raw and sealed key bundles, both directions")
subprocess.run([str(executables["candidate"]), "key-bounds"], check=True, timeout=20)
checked(
    "untrusted key decoder rejects impossible lengths without header-driven allocation"
)
frozen = {}
for name in ["hiqlite", "wal", "peer"]:
    data = json.loads((work / "legacy-fixtures" / (name + ".json")).read_text(encoding="utf-8"))
    frozen[name] = {
        key: {
            "bytes_hex": bytes(value[0]).hex(),
            "prefix_cases": len(value[1]),
            "outcomes_sha256": hashlib.sha256(
                json.dumps(value[1], ensure_ascii=True, separators=(",", ":")).encode()
            ).hexdigest(),
        }
        for key, value in data.items()
    }
frozen["keys_hex"] = (work / "legacy-fixtures/keys.bin").read_bytes().hex()
frozen["public_key_error_sha256"] = hashlib.sha256(
    (work / "legacy-fixtures/key-errors.json").read_bytes()
).hexdigest()
fixture = root / "scripts/bincode-compatibility" / ("legacy-fixtures" + tag + ".json")
if a.freeze:
    assert not fixture.exists(), "Never silently overwrite frozen baseline fixtures"
    fixture.write_text(json.dumps(frozen, indent=2, sort_keys=True) + "\n", encoding="utf-8")
assert json.loads(fixture.read_text(encoding="utf-8")) == frozen, "Independent legacy fixtures drifted"
checked("frozen genuine legacy bytes and malformed-input outcomes")
if a.fixtures_only:
    raise SystemExit(0)


class Node:
    def __init__(self, side, node_id, data, nodes, tag):
        self.data = pathlib.Path(data)
        self.data.mkdir(parents=True, exist_ok=True)
        self.id = node_id
        self.queue = queue.Queue()
        self.log = (work / (tag + ".stderr.log")).open("w", encoding="utf8")
        self.process = subprocess.Popen(
            [
                str(executables[side]),
                "node",
                str(node_id),
                str(self.data),
                json.dumps(nodes),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log,
            encoding="utf8",
            bufsize=1,
        )

        def read():
            for line in self.process.stdout:
                if line.startswith("COMPAT:"):
                    self.queue.put(json.loads(line[7:]))
            self.queue.put({"process_exit": self.process.poll()})

        threading.Thread(target=read, daemon=True).start()

    def response(self, timeout=65):
        result = self.queue.get(timeout=timeout)
        assert (
            "process_exit" not in result if isinstance(result, dict) else True
        ), result
        return result

    def ready(self):
        assert self.response() == {"ready": self.id}

    def cmd(self, op, **args):
        self.process.stdin.write(json.dumps({"op": op, **args}) + "\n")
        self.process.stdin.flush()
        return self.response()

    def kill(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=15)
        self.log.close()

    def stop(self):
        assert self.cmd("shutdown") == "stopped"
        # A probe that panics while shutting down must not pass.
        assert self.process.wait(timeout=15) == 0, "probe exited non-zero after shutdown"
        self.log.close()


def topology(count):
    reservations = []
    nodes = []
    for i in range(count):
        ports = []
        for _ in range(2):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            reservations.append(sock)
            ports.append(sock.getsockname()[1])
        nodes.append(
            {
                "id": i + 1,
                "addr_raft": f"127.0.0.1:{ports[0]}",
                "addr_api": f"127.0.0.1:{ports[1]}",
            }
        )
    return nodes, reservations


def assert_rows(node, ids):
    rows = node.cmd("read")
    assert [r["id"] for r in rows] == ids, rows
    for r in rows:
        i = r["id"]
        assert r == {
            "id": i,
            "text": f"row-{i}-é-💾",
            "num": -(2**63) + i,
            "real": -123.5,
            "blob": list(range(256)),
            "optional": None,
        }, r


def assert_snapshot_received(node, minimum, tag):
    deadline = time.monotonic() + 20
    while True:
        metrics = node.cmd("metrics")
        status = node.cmd("transport")
        received = [
            o
            for o in status["observations"]
            if o["raft_group"] == "sqlite"
            and o["direction"] == "inbound"
            and o["phase"] == "complete"
            and (o.get("locally_received_bytes") or 0) > 0
        ]
        if (
            metrics["snapshot"] is not None
            and metrics["snapshot"]["index"] >= minimum
            and received
        ):
            (work / (tag + "-snapshot-evidence.json")).write_text(
                json.dumps({"metrics": metrics, "transport": status}, indent=2) + "\n",
                encoding="utf-8",
            )
            return
        assert time.monotonic() < deadline, (metrics, status)
        time.sleep(0.025)


def stage_wal_only(node, row_id, tag):
    evidence = node.cmd("stage-unapplied", id=row_id)
    import sqlite3

    database = next(node.data.rglob("compat.db"))
    with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True) as db:
        assert (
            db.execute("SELECT count(*) FROM compat WHERE id=?", (row_id,)).fetchone()[
                0
            ]
            == 0
        ), "row was already present in SQLite"
    assert (
        evidence["wal"]["last_durable_index"]
        >= evidence["index"]
        > evidence["last_applied"]["index"]
    )
    (work / (tag + "-wal-replay-evidence.json")).write_text(
        json.dumps(evidence, indent=2) + "\n",
        encoding="utf-8",
    )


def inspection(exe, path):
    done = subprocess.run(
        [str(exe), "inspect", str(path)],
        encoding="utf8",
        capture_output=True,
        check=True,
        timeout=20,
    )
    return json.loads(
        next(
            line[7:] for line in done.stdout.splitlines() if line.startswith("COMPAT:")
        )
    )


active = []
try:
    nodes, reservations = topology(1)
    for s in reservations:
        s.close()
    data = work / "single-node"
    assert not data.exists(), "Use a fresh owned validation directory"
    node = Node("legacy", 1, data, nodes, "single-old-write")
    active.append(node)
    node.ready()
    for i in range(1, 4):
        node.cmd("seed", id=i)
    assert_rows(node, [1, 2, 3])
    stage_wal_only(node, 4, "old-to-new")
    node.kill()
    metadata = next(data.rglob("meta.hql"))
    wal = metadata.parent
    pristine = inspection(executables["legacy"], wal)
    assert pristine == inspection(executables["candidate"], wal)
    # Equality alone would pass if both probes failed the same way.
    assert "ok" in pristine and pristine["ok"]["invariant_verdicts"], pristine
    checked("old real WAL, vote, and standard metadata inspect identically")
    node = Node("candidate", 1, data, nodes, "single-new-recover")
    active.append(node)
    node.ready()
    assert_rows(node, [1, 2, 3, 4])
    node.cmd("seed", id=5)
    stage_wal_only(node, 6, "new-to-old")
    node.kill()
    node = Node("legacy", 1, data, nodes, "single-old-recover-new-wal")
    active.append(node)
    node.ready()
    assert_rows(node, [1, 2, 3, 4, 5, 6])
    node.cmd("snapshot")
    node.stop()
    checked("crash/restart and real WAL replay old→new and new→old")
    node = Node("candidate", 1, data, nodes, "single-new-snapshot-recover")
    active.append(node)
    node.ready()
    assert_rows(node, [1, 2, 3, 4, 5, 6])
    node.cmd("seed", id=7)
    node.cmd("snapshot")
    node.kill()
    node = Node("legacy", 1, data, nodes, "single-old-snapshot-recover")
    active.append(node)
    node.ready()
    assert_rows(node, [1, 2, 3, 4, 5, 6, 7])
    node.stop()
    checked("published SQLite/Raft snapshot and state metadata recover both directions")
    # Reproduce a process interruption before metadata rename and before the
    # WAL writer publishes the new record id. Committed bytes remain intact.
    for side in ["candidate", "legacy"]:
        copy = work / ("interrupted-write-" + side)
        shutil.copytree(data, copy)
        meta = next(copy.rglob("meta.hql"))
        (meta.parent / "meta.hql.tmp").write_bytes(b"HQLMETA\x01\0")
        used = []
        for f in meta.parent.iterdir():
            if f.is_file():
                content = f.read_bytes()
                if (
                    content.startswith(b"HQL_WAL\x01")
                    and int.from_bytes(content[28:32], "big") > 0
                ):
                    used.append(f)
        assert used, "No real WAL record available for interruption fixture"
        f = sorted(used)[-1]
        content = bytearray(f.read_bytes())
        start = int.from_bytes(content[28:32], "big") + 1
        assert start + 20 < len(content) and content[start : start + 8] == bytes(8)
        # Payload/length stores precede the id store in WalFile::append.
        content[start + 8 : start + 12] = b"\x01\x02\x03\x04"
        content[start + 12 : start + 16] = (4096).to_bytes(4, "big")
        content[start + 16 : start + 19] = b"bad"
        f.write_bytes(content)
        n = Node(side, 1, copy, nodes, "interrupted-write-" + side)
        active.append(n)
        n.ready()
        assert_rows(n, [1, 2, 3, 4, 5, 6, 7])
        n.stop()
        checked("actual restart ignores unpublished metadata/WAL tail: " + side)

    # Inspect genuine prior and new files under supported corruption cases.
    # Both inspectors see the identical stopped-node copy; hash verification
    # also enforces the public read-only diagnostic contract.
    for label, mutate in [
        ("truncated-metadata", lambda b: b[:10]),
        ("bad-metadata-crc", lambda b: b[:8] + bytes([b[8] ^ 1]) + b[9:]),
        ("unknown-metadata-version", lambda b: b[:7] + b"\xff" + b[8:]),
    ]:
        target = work / label
        shutil.copytree(wal, target)
        f = target / "meta.hql"
        f.write_bytes(mutate(f.read_bytes()))
        before = {
            str(f.relative_to(target)): hashlib.sha256(f.read_bytes()).hexdigest()
            for f in target.rglob("*")
            if f.is_file()
        }
        old = inspection(executables["legacy"], target)
        new = inspection(executables["candidate"], target)
        assert old == new, (label, old, new)
        assert "error" in old or old["ok"]["invariant_verdicts"], (label, old)
        after = {
            str(f.relative_to(target)): hashlib.sha256(f.read_bytes()).hexdigest()
            for f in target.rglob("*")
            if f.is_file()
        }
        assert before == after
        checked("unchanged bounded read-only failure: " + label)
    # Actual mixed-version sockets, Raft append/vote, error responses and
    # snapshot installation. Roll one member at a time; preserve membership.
    nodes, reservations = topology(3)
    cluster = []
    for i in range(3):
        for s in reservations[i * 2 : i * 2 + 2]:
            s.close()
        node = Node("legacy", i + 1, work / f"mixed-{i+1}", nodes, f"mixed-old-{i+1}")
        cluster.append(node)
        active.append(node)
    for node in cluster:
        node.ready()
    cluster[0].cmd("seed", id=101)
    # Replace a follower with a blank candidate so the existing leader must
    # send it a real, published snapshot instead of merely reusing local DB.
    leader = cluster[0].cmd("metrics")["current_leader"]
    follower = next(i for i in range(3) if i + 1 != leader)
    cluster[follower].kill()
    cluster[leader - 1].cmd("seed", id=102)
    snapshot = cluster[leader - 1].cmd("snapshot")["snapshot_index"]
    shutil.rmtree(work / f"mixed-{follower+1}")
    node = Node(
        "candidate",
        follower + 1,
        work / f"mixed-{follower+1}",
        nodes,
        "mixed-new-from-old-snapshot",
    )
    active.append(node)
    cluster[follower] = node
    node.ready()
    assert_rows(node, [101, 102])
    assert_snapshot_received(node, snapshot, "old-to-new")
    node.cmd("seed", id=103)
    for n in cluster:
        assert_rows(n, [101, 102, 103])
    checked(
        "mixed old/new cluster: old snapshot→new follower and new client→old leader"
    )
    # Roll the remaining old members to candidate, then return one empty
    # follower to the old binary to exercise the reciprocal snapshot path.
    for i in range(3):
        if i == follower:
            continue
        cluster[i].kill()
        node = Node(
            "candidate", i + 1, work / f"mixed-{i+1}", nodes, f"mixed-roll-new-{i+1}"
        )
        active.append(node)
        cluster[i] = node
        node.ready()
        assert_rows(node, [101, 102, 103])
    leader = cluster[0].cmd("metrics")["current_leader"]
    follower = next(i for i in range(3) if i + 1 != leader)
    cluster[follower].kill()
    cluster[leader - 1].cmd("seed", id=104)
    snapshot = cluster[leader - 1].cmd("snapshot")["snapshot_index"]
    shutil.rmtree(work / f"mixed-{follower+1}")
    node = Node(
        "legacy",
        follower + 1,
        work / f"mixed-{follower+1}",
        nodes,
        "mixed-old-from-new-snapshot",
    )
    active.append(node)
    cluster[follower] = node
    node.ready()
    assert_rows(node, [101, 102, 103, 104])
    assert_snapshot_received(node, snapshot, "new-to-old")
    node.cmd("seed", id=105)
    for n in cluster:
        assert_rows(n, [101, 102, 103, 104, 105])
    checked(
        "mixed new/old cluster: new snapshot→old follower and old client→new leader"
    )
    for node in cluster:
        node.stop()
finally:
    for node in active:
        node.kill()
checked("all owned validation processes closed")
