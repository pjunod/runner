//! Built-in HTTPS for the API/UI (`[api] tls = true`).
//!
//! With explicit `tls_cert`/`tls_key` paths, those PEM files are loaded.
//! With neither set, a self-signed certificate is generated ONCE into
//! `<state_dir>/tls/` and reused on every boot, so the fingerprint your
//! devices trusted stays stable. Browsers only grant service workers and
//! PWA install to origins they consider secure — a self-signed cert
//! qualifies once it's trusted on the device (clicking through the
//! warning is not enough on Chrome/Android).

use crate::anyhow_lite;
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use std::path::Path;
use std::sync::Arc;

pub struct TlsSetup {
    pub config: Arc<rustls::ServerConfig>,
    /// Exact known certificate for the private responsiveness check.
    pub certificate: Vec<u8>,
    /// Human-readable note for the startup log (cert origin/fingerprint).
    pub note: String,
}

fn storage_error(op: &str, path: &Path, error: std::io::Error) -> anyhow_lite::Error {
    // Missing explicit material and malformed material are invalid startup
    // configuration; uncertain storage never authorizes identity generation.
    let prefix = if error.kind() == std::io::ErrorKind::NotFound {
        ""
    } else {
        "storage unavailable: "
    };
    err(format!("{prefix}{op} {}: {error}", path.display()))
}

fn err(msg: String) -> anyhow_lite::Error {
    anyhow_lite::Error::msg(msg)
}

/// Select one rustls 0.23 provider before any client or server builder runs.
///
/// Some workspace dependencies enable ring while nzbd uses aws-lc. rustls
/// deliberately refuses to guess when both are compiled, so this process-wide
/// choice is an explicit startup invariant rather than a feature-resolution
/// accident.
pub fn install_process_crypto_provider() -> anyhow_lite::Result<()> {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Ok(());
    }
    match rustls::crypto::aws_lc_rs::default_provider().install_default() {
        Ok(()) => Ok(()),
        Err(_) if rustls::crypto::CryptoProvider::get_default().is_some() => Ok(()),
        Err(_) => Err(err(
            "could not install the process rustls aws-lc crypto provider".into(),
        )),
    }
}

/// Build the server TLS config, generating a persistent self-signed cert
/// when none is configured. Returns None when `[api] tls` is off.
///
/// Existing security material is read without requiring backend ownership.
/// The caller's nonmutating installation preflight runs only immediately
/// before proven first initialization could create material on the data root.
/// This keeps a conflicting inventory owner from withdrawing safe controls.
pub fn server_config_checked(
    cfg: &nzbd_config::Config,
    state_dir: &Path,
    before_initialization: impl FnOnce() -> anyhow_lite::Result<()>,
) -> anyhow_lite::Result<Option<TlsSetup>> {
    install_process_crypto_provider()?;
    if !cfg.api.tls {
        return Ok(None);
    }
    let (cert_path, key_path, generated) = match (&cfg.api.tls_cert, &cfg.api.tls_key) {
        (Some(c), Some(k)) => (
            nzbd_config::expand_home(c),
            nzbd_config::expand_home(k),
            false,
        ),
        (None, None) => {
            let dir = state_dir.join("tls");
            (dir.join("cert.pem"), dir.join("key.pem"), true)
        }
        _ => {
            return Err(err(
                "[api] tls_cert and tls_key must be set together (or neither, for self-signed)"
                    .into(),
            ))
        }
    };

    if generated {
        let cert_exists = cert_path
            .try_exists()
            .map_err(|error| storage_error("inspect", &cert_path, error))?;
        let key_exists = key_path
            .try_exists()
            .map_err(|error| storage_error("inspect", &key_path, error))?;
        match (cert_exists, key_exists) {
            (true, true) => {}
            (false, false) => {
                let directory = cert_path.parent().expect("generated TLS directory");
                match std::fs::metadata(directory) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        before_initialization()?;
                        generate_self_signed(&cert_path, &key_path, &cfg.api.tls_sans)?
                    }
                    Err(error) => return Err(storage_error("inspect", directory, error)),
                    Ok(_) => {
                        return Err(err(format!(
                            "incomplete TLS identity in {}; refusing to replace existing material",
                            directory.display()
                        )))
                    }
                }
            }
            _ => {
                return Err(err(format!(
                    "incomplete TLS certificate/key pair in {}; refusing to regenerate identity",
                    cert_path.parent().unwrap().display()
                )))
            }
        }
    }

    let certs: Vec<CertificateDer<'static>> = {
        let pem =
            std::fs::read(&cert_path).map_err(|error| storage_error("read", &cert_path, error))?;
        CertificateDer::pem_slice_iter(&pem)
            .collect::<Result<_, _>>()
            .map_err(|e| err(format!("parse {}: {e}", cert_path.display())))?
    };
    if certs.is_empty() {
        return Err(err(format!(
            "{}: no certificates found",
            cert_path.display()
        )));
    }
    let key = {
        let pem =
            std::fs::read(&key_path).map_err(|error| storage_error("read", &key_path, error))?;
        PrivateKeyDer::pem_slice_iter(&pem)
            .next()
            .transpose()
            .map_err(|e| err(format!("parse {}: {e}", key_path.display())))?
            .ok_or_else(|| err(format!("{}: no private key found", key_path.display())))?
    };

    let certificate = certs[0].as_ref().to_vec();
    let fingerprint = sha256_hex(&certificate);
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| err(format!("tls config: {e}")))?;
    let note = if generated {
        format!(
            "self-signed cert {} (sha256 {fingerprint}) — trust it on your devices for full PWA",
            cert_path.display()
        )
    } else {
        format!("cert {} (sha256 {fingerprint})", cert_path.display())
    };
    Ok(Some(TlsSetup {
        config: Arc::new(config),
        certificate,
        note,
    }))
}

fn generate_self_signed(
    cert_path: &Path,
    key_path: &Path,
    extra_sans: &[String],
) -> anyhow_lite::Result<()> {
    let mut sans: Vec<String> = vec!["localhost".into(), "nzbd".into()];
    if let Ok(host) = std::env::var("HOSTNAME") {
        if !host.is_empty() {
            sans.push(host);
        }
    }
    sans.extend(extra_sans.iter().cloned());
    sans.dedup();
    let cert = rcgen::generate_simple_self_signed(sans.clone())
        .map_err(|e| err(format!("generate self-signed cert: {e}")))?;
    // Publish the complete identity with one directory rename. The final
    // directory must be absent; rename cannot replace another complete pair
    // because a complete TLS directory is nonempty. An interrupted staging
    // directory is never treated as authority to rotate a persisted identity.
    let directory = cert_path.parent().expect("TLS certificate directory");
    let parent = directory
        .parent()
        .ok_or_else(|| err("TLS directory has no parent".into()))?;
    std::fs::create_dir_all(parent).map_err(|error| storage_error("create", parent, error))?;
    // Serialize cooperating initializers outside the yet-unpublished TLS
    // directory. A stale lock records interrupted initialization and must
    // never be interpreted as permission to create another identity.
    let initialization_lock = parent.join(".nzbd-tls-initialization.lock");
    let lock = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&initialization_lock)
        .map_err(|error| {
            storage_error(
                "claim first TLS initialization",
                &initialization_lock,
                error,
            )
        })?;
    if directory
        .try_exists()
        .map_err(|error| storage_error("inspect", directory, error))?
    {
        return Err(err(format!(
            "TLS identity appeared during initialization in {}; refusing replacement",
            directory.display()
        )));
    }

    let staging = parent.join(format!(
        ".tls-initializing-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir(&staging).map_err(|error| storage_error("create", &staging, error))?;
    let staged_cert = staging.join(cert_path.file_name().unwrap());
    let staged_key = staging.join(key_path.file_name().unwrap());
    write_exclusive(&staged_cert, cert.cert.pem().as_bytes(), false)?;
    write_exclusive(&staged_key, cert.key_pair.serialize_pem().as_bytes(), true)?;
    sync_directory(&staging)?;
    std::fs::rename(&staging, directory)
        .map_err(|error| storage_error("publish TLS identity", directory, error))?;
    sync_directory(parent)?;
    drop(lock);
    std::fs::remove_file(&initialization_lock)
        .map_err(|error| storage_error("finish TLS initialization", &initialization_lock, error))?;
    sync_directory(parent)?;
    tracing::info!(
        cert = %cert_path.display(),
        sans = %sans.join(", "),
        "generated self-signed TLS certificate (persisted; reused on next boot)"
    );
    Ok(())
}

/// Exclusive creation with key permissions set before any secret bytes exist.
fn write_exclusive(path: &Path, bytes: &[u8], private: bool) -> anyhow_lite::Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(if private { 0o600 } else { 0o644 });
    }
    #[cfg(not(unix))]
    let _ = private;
    let mut file = options
        .open(path)
        .map_err(|error| storage_error("create", path, error))?;
    file.write_all(bytes)
        .map_err(|error| storage_error("write", path, error))?;
    file.sync_all()
        .map_err(|error| storage_error("sync", path, error))?;
    Ok(())
}
fn sync_directory(path: &Path) -> anyhow_lite::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| storage_error("sync directory", path, error))?;
    Ok(())
}

fn sha256_hex(der: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    Sha256::digest(der)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cfg(dir: &Path) -> nzbd_config::Config {
        let mut cfg = nzbd_config::Config::default();
        cfg.paths.main_dir = dir.join("data");
        cfg.paths.dest_dir = dir.join("data/complete");
        cfg
    }

    #[test]
    fn tls_off_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = base_cfg(tmp.path());
        assert!(server_config_checked(&cfg, tmp.path(), || Ok(()))
            .unwrap()
            .is_none());
    }

    #[test]
    fn self_signed_generated_once_and_reused() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = base_cfg(tmp.path());
        cfg.api.tls = true;
        cfg.api.tls_sans = vec!["nas.lan".into()];

        let first = server_config_checked(&cfg, tmp.path(), || Ok(()))
            .unwrap()
            .unwrap();
        assert!(first.note.contains("self-signed"));
        assert!(first.note.contains("sha256"));
        let cert_pem = std::fs::read_to_string(tmp.path().join("tls/cert.pem")).unwrap();
        assert!(cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(tmp.path().join("tls/key.pem").exists());

        // Key stays private on unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(tmp.path().join("tls/key.pem"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "key must be 0600");
        }

        // Second boot: same cert (fingerprint in the note is identical).
        let second = server_config_checked(&cfg, tmp.path(), || Ok(()))
            .unwrap()
            .unwrap();
        let fp = |note: &str| note.split("sha256 ").nth(1).map(str::to_string);
        assert_eq!(fp(&first.note), fp(&second.note), "cert must persist");
    }

    #[test]
    fn custom_cert_pair_must_be_set_together() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = base_cfg(tmp.path());
        cfg.api.tls = true;
        cfg.api.tls_cert = Some(std::path::PathBuf::from("/x/cert.pem"));
        let err = match server_config_checked(&cfg, tmp.path(), || Ok(())) {
            Err(e) => e,
            Ok(_) => panic!("half-configured pair must error"),
        };
        assert!(err.to_string().contains("together"), "{err}");
    }

    #[test]
    fn custom_cert_loads_and_bad_pem_fails_loudly() {
        let tmp = tempfile::tempdir().unwrap();

        // Generate a valid pair by running self-signed once...
        let mut gen = base_cfg(tmp.path());
        gen.api.tls = true;
        server_config_checked(&gen, tmp.path(), || Ok(()))
            .unwrap()
            .unwrap();

        // ...then load it back through the bring-your-own path.
        let mut cfg = base_cfg(tmp.path());
        cfg.api.tls = true;
        cfg.api.tls_cert = Some(tmp.path().join("tls/cert.pem"));
        cfg.api.tls_key = Some(tmp.path().join("tls/key.pem"));
        let setup = server_config_checked(&cfg, tmp.path(), || Ok(()))
            .unwrap()
            .unwrap();
        assert!(!setup.note.contains("self-signed"));

        // Garbage cert file: a clear parse/empty error, not a panic.
        std::fs::write(tmp.path().join("tls/cert.pem"), "not a pem").unwrap();
        let err = match server_config_checked(&cfg, tmp.path(), || Ok(())) {
            Err(e) => e,
            Ok(_) => panic!("garbage cert must error"),
        };
        assert!(err.to_string().contains("no certificates"), "{err}");

        // Missing key file: the io error names the path.
        std::fs::write(
            tmp.path().join("tls/cert.pem"),
            std::fs::read(tmp.path().join("tls/key.pem")).unwrap(),
        )
        .unwrap();
        std::fs::remove_file(tmp.path().join("tls/key.pem")).unwrap();
        assert!(server_config_checked(&cfg, tmp.path(), || Ok(())).is_err());
    }

    #[test]
    fn incomplete_identity_never_regenerates_or_overwrites_surviving_key() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = base_cfg(tmp.path());
        cfg.api.tls = true;
        server_config_checked(&cfg, tmp.path(), || Ok(())).unwrap();
        let key_path = tmp.path().join("tls/key.pem");
        let key = std::fs::read(&key_path).unwrap();
        std::fs::remove_file(tmp.path().join("tls/cert.pem")).unwrap();
        assert!(server_config_checked(&cfg, tmp.path(), || Ok(())).is_err());
        assert_eq!(std::fs::read(key_path).unwrap(), key);
        assert!(!tmp.path().join("tls/cert.pem").try_exists().unwrap());
    }

    #[test]
    fn sha256_hex_format() {
        let h = sha256_hex(b"nzbd");
        assert_eq!(h.len(), 32 * 3 - 1, "colon-separated hex bytes");
        assert!(h.split(':').all(|b| b.len() == 2));
    }
}
