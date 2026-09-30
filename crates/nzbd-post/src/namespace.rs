//! One bounded, no-symlink inventory for all post-processing stages.
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

pub fn relative(name: &str) -> io::Result<PathBuf> {
    if name.len() > 4096 || name.contains('\\') || name.contains(':') || name.contains('\0') {
        return Err(io::Error::other("unsafe catalog name"));
    }
    let p = Path::new(name);
    if p.components().count() > 32
        || p.components().count() == 0
        || p.components().any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(io::Error::other("unsafe catalog path"));
    }
    Ok(p.into())
}

pub fn files(root: &Path) -> io::Result<Vec<PathBuf>> {
    fn walk(
        dir: &Path,
        depth: usize,
        entries: &mut usize,
        out: &mut Vec<PathBuf>,
    ) -> io::Result<()> {
        if depth > 32 {
            return Err(io::Error::other("inventory depth limit"));
        }
        if std::fs::symlink_metadata(dir)?.file_type().is_symlink() {
            return Err(io::Error::other("symlink inventory root"));
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            *entries += 1;
            if *entries > 100_000 {
                return Err(io::Error::other("inventory entry limit"));
            }
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(".pp.")
                || name.to_string_lossy().starts_with(".runner-")
            {
                continue;
            }
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Err(io::Error::other("symlink inventory entry"));
            }
            if kind.is_dir() {
                walk(&entry.path(), depth + 1, entries, out)?;
            } else if kind.is_file() {
                out.push(entry.path());
            } else {
                return Err(io::Error::other("unsupported inventory entry"));
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(root, 0, &mut 0, &mut out)?;
    out.sort();
    Ok(out)
}

pub fn equal_files(a: &Path, b: &Path) -> io::Result<bool> {
    let mut a = nzbd_state::fileops::open(a).map_err(io::Error::other)?;
    let mut b = nzbd_state::fileops::open(b).map_err(io::Error::other)?;
    if a.metadata()?.len() != b.metadata()?.len() {
        return Ok(false);
    }
    let mut x = [0; 65536];
    let mut y = [0; 65536];
    loop {
        let n = a.read(&mut x)?;
        b.read_exact(&mut y[..n])?;
        if x[..n] != y[..n] {
            return Ok(false);
        }
        if n == 0 {
            return Ok(true);
        }
    }
}
