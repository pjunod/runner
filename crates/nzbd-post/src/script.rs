//! NZBGet extension-script protocol (ARCHITECTURE.md §3.2 "Script
//! protocol"): env-var interface, `[LEVEL]` stdout log lines, the
//! `[NZB] KEY=value` command channel, exit codes 92–95. Existing NZBGet
//! post-processing scripts run unmodified.

use crate::{PostError, ScriptOutcome};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};

pub struct ScriptHost {
    pub timeout: Duration,
}

/// Spawn a child, retrying briefly on `ETXTBSY` ("Text file busy").
///
/// A script that was JUST written is executable but not always
/// exec-able: if any thread in this process still holds a write handle to
/// the file when another thread forks, the child inherits that handle and
/// its `execve` fails with `ETXTBSY`. Nothing about the script is wrong,
/// and the same call succeeds milliseconds later — which is exactly the
/// shape of failure that reads as "my post-processing script randomly
/// doesn't run".
///
/// nzbd is squarely in the path of this: it runs operator scripts, and an
/// operator who drops a new script into `scripts_dir` while a job is
/// finishing is racing precisely this window. Retrying is the accepted
/// mitigation (rust-lang/rust#114554); a handful of short waits covers
/// the fork window without turning a genuinely missing interpreter into a
/// long stall.
async fn spawn_retrying_etxtbsy(
    cmd: &mut tokio::process::Command,
) -> std::io::Result<tokio::process::Child> {
    const WAITS_MS: [u64; 4] = [1, 5, 20, 50];
    for wait in WAITS_MS {
        match cmd.spawn() {
            Err(e) if e.raw_os_error() == Some(26) => {
                tokio::time::sleep(Duration::from_millis(wait)).await;
            }
            other => return other,
        }
    }
    cmd.spawn()
}

impl ScriptHost {
    /// Run one script with the given environment (`NZBOP_*`/`NZBPP_*`/…
    /// built by the caller). Stdout is parsed line-by-line.
    ///
    /// See [`spawn_retrying_etxtbsy`] for why the spawn is not a plain
    /// `cmd.spawn()`.
    pub async fn run(
        &self,
        entry: &Path,
        cwd: &Path,
        env: &[(String, String)],
    ) -> Result<ScriptOutcome, PostError> {
        let control = crate::attempt::current();
        let cancellation = control
            .as_ref()
            .map_or_else(tokio_util::sync::CancellationToken::new, |control| {
                control.cancel.child_token()
            });
        let _cancel_on_drop = cancellation.clone().drop_guard();
        let entry = entry.to_path_buf();
        let cwd = cwd.to_path_buf();
        let env = env.to_vec();
        let timeout = self.timeout;
        let guard = control
            .as_ref()
            .and_then(|control| control.use_guard.lock().unwrap().clone());
        let worker_control = control.clone();
        let run = async move {
            let _guard = guard;
            let result = ScriptHost { timeout }
                .run_owned(&entry, &cwd, &env, cancellation)
                .await;
            if let Some(control) = worker_control {
                control.preserve_uncertainty(&result).await;
            }
            result
        };
        let task = if let Some(control) = control {
            control.workers.spawn(run)
        } else {
            tokio::spawn(run)
        };
        task.await
            .map_err(|e| PostError::Subprocess(format!("script worker: {e}")))?
    }
    async fn run_owned(
        &self,
        entry: &Path,
        cwd: &Path,
        env: &[(String, String)],
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<ScriptOutcome, PostError> {
        let mut cmd = tokio::process::Command::new(entry);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.as_std_mut().process_group(0);
        }
        cmd.current_dir(cwd)
            .env_clear()
            .env(
                "PATH",
                std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into()),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = spawn_retrying_etxtbsy(&mut cmd)
            .await
            .map_err(|e| PostError::ToolMissing(format!("{}: {e}", entry.display())))?;

        let group = child.id();
        let stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let script = entry
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();

        let parse = async {
            let mut commands = Vec::new();
            let mut lines = BufReader::new(stdout).lines();
            let mut seen = 0usize;
            while let Ok(Some(line)) = lines.next_line().await {
                seen += 1;
                if seen > 100_000 {
                    break; // output cap
                }
                if let Some(rest) = line.strip_prefix("[NZB] ") {
                    if let Some((k, v)) = rest.split_once('=') {
                        commands.push((k.trim().to_string(), v.trim().to_string()));
                    }
                } else if let Some(rest) = line.strip_prefix("[ERROR] ") {
                    tracing::error!(%script, "{rest}");
                } else if let Some(rest) = line.strip_prefix("[WARNING] ") {
                    tracing::warn!(%script, "{rest}");
                } else if let Some(rest) = line.strip_prefix("[INFO] ") {
                    tracing::info!(%script, "{rest}");
                } else if let Some(rest) = line
                    .strip_prefix("[DETAIL] ")
                    .or_else(|| line.strip_prefix("[DEBUG] "))
                {
                    tracing::debug!(%script, "{rest}");
                } else if !line.is_empty() {
                    tracing::debug!(%script, "{line}");
                }
            }
            commands
        };

        let result = tokio::select! {
            _ = cancellation.cancelled() => None,
            result = tokio::time::timeout(self.timeout, async {
            let mut sink = tokio::io::sink();
            let (commands, status, _) = tokio::join!(parse, child.wait(), tokio::io::copy(&mut stderr, &mut sink));
            (commands, status)
        }) => Some(result),
        };

        match result {
            Some(Ok((commands, Ok(status)))) => {
                crate::attempt::kill_child(&mut child, group).await?;
                Ok(ScriptOutcome {
                    exit_code: status.code().unwrap_or(-1),
                    commands,
                })
            }
            Some(Ok((_, Err(e)))) => Err(PostError::Subprocess(format!("{script}: {e}"))),
            Some(Err(_)) => {
                crate::attempt::kill_child(&mut child, group).await?;
                Err(PostError::Subprocess(format!("{script}: timed out")))
            }
            None => {
                crate::attempt::kill_child(&mut child, group).await?;
                Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "script cancelled").into())
            }
        }
    }
}

/// Discover post-processing scripts: executables in `dir` carrying the
/// legacy `### NZBGET POST-PROCESSING SCRIPT ###` header, or v2 extension
/// directories with a `manifest.json` naming a `main` entry.
pub fn discover(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();
    for p in paths {
        if p.is_dir() {
            let manifest = p.join("manifest.json");
            if let Ok(bytes) = std::fs::read(&manifest) {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    if let Some(main) = v["main"].as_str() {
                        let entry = p.join(main);
                        if entry.is_file() {
                            out.push(entry);
                        }
                    }
                }
            }
        } else if p.is_file() {
            if let Ok(head) = std::fs::read_to_string(&p) {
                if head.contains("### NZBGET POST-PROCESSING SCRIPT ###") {
                    out.push(p);
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn write_script(path: &Path, body: &str) {
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[tokio::test]
    async fn protocol_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("notify.sh");
        write_script(
            &script,
            "#!/bin/sh\n\
             ### NZBGET POST-PROCESSING SCRIPT ###\n\
             echo \"[INFO] hello from $NZBPP_NZBNAME\"\n\
             echo \"[NZB] MARK=yes\"\n\
             echo \"[NZB] FINALDIR=$NZBPP_DIRECTORY/moved\"\n\
             exit 93\n",
        );
        let host = ScriptHost {
            timeout: Duration::from_secs(10),
        };
        let out = host
            .run(
                &script,
                tmp.path(),
                &[
                    ("NZBPP_NZBNAME".into(), "myjob".into()),
                    ("NZBPP_DIRECTORY".into(), "/dest/myjob".into()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(out.exit_code, crate::script_exit::SUCCESS);
        assert_eq!(
            out.commands,
            vec![
                ("MARK".to_string(), "yes".to_string()),
                ("FINALDIR".to_string(), "/dest/myjob/moved".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn exit_codes_pass_through() {
        let tmp = tempfile::tempdir().unwrap();
        for (code, name) in [(94, "err.sh"), (95, "skip.sh"), (92, "parreq.sh")] {
            let p = tmp.path().join(name);
            write_script(&p, &format!("#!/bin/sh\nexit {code}\n"));
            let host = ScriptHost {
                timeout: Duration::from_secs(5),
            };
            let out = host.run(&p, tmp.path(), &[]).await.unwrap();
            assert_eq!(out.exit_code, code);
        }
    }

    #[test]
    fn discovery_legacy_and_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        write_script(
            &tmp.path().join("legacy.sh"),
            "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\nexit 93\n",
        );
        std::fs::write(tmp.path().join("not-a-script.txt"), "hello").unwrap();
        let ext = tmp.path().join("myext");
        std::fs::create_dir(&ext).unwrap();
        std::fs::write(
            ext.join("manifest.json"),
            r#"{"main": "main.sh", "name": "MyExt"}"#,
        )
        .unwrap();
        write_script(&ext.join("main.sh"), "#!/bin/sh\nexit 93\n");

        let found = discover(tmp.path());
        let names: Vec<String> = found
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["legacy.sh", "main.sh"]);
    }

    #[tokio::test]
    async fn protocol_ignores_malformed_commands_and_bounds_runtime() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("all-lines.sh");
        write_script(
            &script,
            "#!/bin/sh\n\
             echo '[ERROR] error'\n\
             echo '[WARNING] warning'\n\
             echo '[INFO] info'\n\
             echo '[DETAIL] detail'\n\
             echo '[DEBUG] debug'\n\
             echo 'plain output'\n\
             echo '[NZB] malformed'\n\
             echo '[NZB] KEY = value'\n",
        );
        let host = ScriptHost {
            timeout: Duration::from_secs(2),
        };
        let out = host.run(&script, tmp.path(), &[]).await.unwrap();
        assert_eq!(out.exit_code, 0);
        assert_eq!(out.commands, vec![("KEY".into(), "value".into())]);

        let noisy = tmp.path().join("noisy.sh");
        write_script(
            &noisy,
            "#!/bin/sh\nyes plain | head -n 100000\necho '[NZB] LATE=1'\n",
        );
        let host = ScriptHost {
            timeout: Duration::from_secs(10),
        };
        let out = host.run(&noisy, tmp.path(), &[]).await.unwrap();
        assert_eq!(out.exit_code, 0);
        assert!(out.commands.is_empty());

        let hangs = tmp.path().join("hangs.sh");
        write_script(&hangs, "#!/bin/sh\nwhile :; do :; done\n");
        let host = ScriptHost {
            timeout: Duration::from_millis(20),
        };
        let err = host.run(&hangs, tmp.path(), &[]).await.unwrap_err();
        assert!(matches!(err, PostError::Subprocess(message) if message.contains("timed out")));
    }

    #[test]
    fn discovery_skips_missing_malformed_and_incomplete_entries() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(discover(&tmp.path().join("missing")).is_empty());

        let malformed = tmp.path().join("malformed");
        std::fs::create_dir(&malformed).unwrap();
        std::fs::write(malformed.join("manifest.json"), b"not json").unwrap();

        let missing_entry = tmp.path().join("missing-entry");
        std::fs::create_dir(&missing_entry).unwrap();
        std::fs::write(
            missing_entry.join("manifest.json"),
            br#"{"main":"absent.sh"}"#,
        )
        .unwrap();

        // Non-UTF-8 *contents*: the header probe reads the file as a string,
        // so this one fails to decode rather than failing to match. Both
        // arms must skip the file; neither may abort the scan.
        std::fs::write(tmp.path().join("undecodable-contents"), [0xff, 0xfe]).unwrap();
        std::fs::write(tmp.path().join("ordinary.txt"), "ordinary").unwrap();

        assert!(discover(tmp.path()).is_empty());

        // Every rejection above has to be a rejection of that entry and not
        // of the scan: a v2 extension directory sitting among them is still
        // found. Its name is deliberately non-ASCII — the manifest's `main`
        // is joined onto a `Path`, never round-tripped through `str`.
        //
        // (A name that is not valid UTF-8 at all would test the same join
        // more sharply, but APFS refuses to create one — `mkdir` fails
        // EILSEQ — so it cannot be exercised portably.)
        let extension = tmp.path().join("séparé-ext");
        std::fs::create_dir(&extension).unwrap();
        write_script(&extension.join("main.sh"), "#!/bin/sh\nexit 0\n");
        std::fs::write(extension.join("manifest.json"), br#"{"main":"main.sh"}"#).unwrap();
        assert_eq!(discover(tmp.path()), vec![extension.join("main.sh")]);
    }
}
