"""Build the same validation probe against real prior and candidate sources."""

import argparse, json, os, pathlib, shutil, subprocess, tarfile, io, sys

p = argparse.ArgumentParser()
p.add_argument("--work-root", required=True)
p.add_argument("--side", choices=["legacy", "candidate"], required=True)
p.add_argument("--update-locks", action="store_true")
p.add_argument("--profile", choices=["sqlite", "cache-backup"], default="sqlite")
a = p.parse_args()
tag = "" if a.profile == "sqlite" else "-cache-backup"
features = ["sqlite", "macros", "auto-heal", "validation-test-helpers"] + (
    ["cache", "backup"] if tag else []
)
root = pathlib.Path(__file__).resolve().parents[2]
work = pathlib.Path(a.work_root).resolve()
work.mkdir(parents=True, exist_ok=True)
baseline = "5e44e1acfb590c8657a10cf7983f8c37cb04c654"
if a.side == "legacy":
    source = work / "legacy-source"
    if not source.exists():
        source.mkdir()
        data = subprocess.check_output(
            [
                "git",
                "archive",
                baseline,
                "vendor/hiqlite",
                "vendor/hiqlite-wal",
                "contrib/rqbit/vendor",
            ],
            cwd=root,
        )
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            archive.extractall(source, filter="data")
    codec = 'bincode = { version = "=2.0.1" }'
    cryptr = 'cryptr = { version = "=0.10.0" }'
else:
    source = root
    subprocess.run(
        [
            sys.executable,
            str(root / "scripts/bincode-compatibility/check-cryptr-source.py"),
            "--work-root",
            str(work / "source-proof"),
        ],
        check=True,
        cwd=root,
    )
    codec = 'bincode = { package = "grovedb-bincode", version = "=2.1.0" }'
    cryptr = f'cryptr = {{ path = {json.dumps(str(source/"vendor/cryptr"))} }}'
if a.side == "legacy":
    for lib in ["hiqlite", "hiqlite-wal"]:
        shutil.copyfile(
            root / f"vendor/{lib}/src/codec_compatibility.rs",
            source / f"vendor/{lib}/src/codec_compatibility.rs",
        )
        f = source / f"vendor/{lib}/src/lib.rs"
        if "pub mod codec_compatibility;" not in f.read_text(encoding="utf-8"):
            f.write_text(
                f.read_text(encoding="utf-8")
                + '\n#[cfg(feature = "validation-test-helpers")]\npub mod codec_compatibility;\n'
            )
    # Windows platform fixes carried by the oracle; none touches a codec. The
    # baseline (1) synced snapshot files through a read-only handle, which
    # Windows rejects ("Access is denied", os error 5), and (2) found WAL file
    # names by splitting on '/', so native Windows paths failed inspection.
    # Apply the exact candidate fixes so the Windows proof exercises the old
    # codecs instead of these unrelated defects; refuse to build if the
    # baseline text ever differs.
    platform_fixes = [
        (
            "vendor/hiqlite/src/store/state_machine/sqlite/snapshot_builder.rs",
            "pub(crate) async fn sync_file(path: &str) -> StorageResult<()> {\n"
            "    let file = fs::File::open(path)\n",
            "pub(crate) async fn sync_file(path: &str) -> StorageResult<()> {\n"
            "    let file = fs::OpenOptions::new()\n        .write(true)\n        .open(path)\n",
        ),
        (
            "vendor/hiqlite-wal/src/wal.rs",
            "        let Some((_, fname)) = path_full.rsplit_once('/') else {\n",
            "        let Some(fname) = std::path::Path::new(&path_full)\n"
            "            .file_name()\n"
            "            .and_then(|name| name.to_str())\n"
            "        else {\n",
        ),
    ]
    for relative, baseline_text, fixed_text in platform_fixes:
        f = source / relative
        text = f.read_text(encoding="utf-8")
        if fixed_text not in text:
            assert text.count(baseline_text) == 1, f"Baseline {relative} changed; review the oracle fix"
            f.write_text(text.replace(baseline_text, fixed_text), encoding="utf-8")
    f = source / "vendor/hiqlite-wal/Cargo.toml"
    if "validation-test-helpers = []" not in f.read_text(encoding="utf-8"):
        f.write_text(
            f.read_text(encoding="utf-8").replace(
                "[features]\n", "[features]\nvalidation-test-helpers = []\n"
            )
        )
project = work / (a.side + "-probe" + tag)
project.mkdir(exist_ok=True)
manifest = f"""[package]
name = "runner-codec-compatibility-probe"
version = "0.0.0"
edition = "2021"
publish = false
[workspace]
[features]
cache-backup = []
[dependencies]
hiqlite = {{ path = {json.dumps(str(source/'vendor/hiqlite'))}, default-features = false, features = {json.dumps(features)} }}
hiqlite-wal = {{ path = {json.dumps(str(source/'vendor/hiqlite-wal'))}, features = ["validation-test-helpers"] }}
peer = {{ package = "librqbit-peer-protocol", path = {json.dumps(str(source/'contrib/rqbit/vendor/crates/peer_binary_protocol'))}, default-features = false, features = ["sha1-ring"] }}
buffers = {{ package = "librqbit-buffers", path = {json.dumps(str(source/'contrib/rqbit/vendor/crates/buffers'))} }}
tokio = {{ version = "1", features = ["full"] }}
serde_json = "1"
{codec}
{cryptr}
[patch.crates-io]
hiqlite-wal = {{ path = {json.dumps(str(source/'vendor/hiqlite-wal'))} }}
"""
(project / "Cargo.toml").write_text(manifest, encoding="utf-8")
(project / "src").mkdir(exist_ok=True)
shutil.copyfile(
    root / "scripts/bincode-compatibility/probe.rs", project / "src/main.rs"
)
# Never create an internal/default target directory.
assert os.environ.get(
    "CARGO_TARGET_DIR"
), "Set the existing external/hosted compile slot explicitly"
lock = root / "scripts/bincode-compatibility/locks" / (a.side + tag) / "Cargo.lock"
if a.update_locks:
    lock.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["cargo", "generate-lockfile", "--manifest-path", str(project / "Cargo.toml")],
        check=True,
        cwd=root,
    )
    shutil.copyfile(project / "Cargo.lock", lock)
assert lock.is_file(), "Reviewed legacy/candidate probe lockfile is required"
shutil.copyfile(lock, project / "Cargo.lock")
subprocess.run(
    ["cargo", "build", "--locked", "--manifest-path", str(project / "Cargo.toml")]
    + (["--features", "cache-backup"] if tag else []),
    check=True,
    cwd=root,
)
metadata = json.loads(
    subprocess.check_output(
        [
            "cargo",
            "metadata",
            "--locked",
            "--format-version",
            "1",
            "--manifest-path",
            str(project / "Cargo.toml"),
        ],
        cwd=root,
        encoding="utf-8",
    )
)
versions = {(p["name"], p["version"]) for p in metadata["packages"]}
if a.side == "legacy":
    assert {("bincode", "1.3.3"), ("bincode", "2.0.1")} <= versions
else:
    assert not any(name == "bincode" for name, version in versions)
    assert {("fugue-bincode", "1.3.4"), ("grovedb-bincode", "2.1.0")} <= versions
(work / (a.side + "-dependency-graph.json")).write_text(
    json.dumps(metadata, indent=2) + "\n",
    encoding="utf-8",
)
exe = (
    pathlib.Path(os.environ["CARGO_TARGET_DIR"])
    / "debug"
    / ("runner-codec-compatibility-probe" + (".exe" if os.name == "nt" else ""))
)
(work / "bin").mkdir(exist_ok=True)
output = work / "bin" / (a.side + "-probe" + tag + (".exe" if os.name == "nt" else ""))
shutil.copyfile(exe, output)
output.chmod(0o755)
print(output)
