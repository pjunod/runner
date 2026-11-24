//! Docs index law (pipeline contract v2, M6): `docs/README.md` links every
//! Markdown file under `docs/`, and every relative link in `docs/**/*.md`
//! and the top-level `README.md` resolves to a file or directory in the
//! repository. Anchors (`#…`) are not checked; the path before them is.
//!
//! Links inside fenced code blocks and inline code spans are not links.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn markdown_under(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            markdown_under(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "md") {
            out.push(path);
        }
    }
}

/// Lexical normalisation (no filesystem access): `a/./b/../c` → `a/c`.
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Relative link targets in a Markdown document, with their line numbers.
fn relative_links(text: &str) -> Vec<(usize, String)> {
    let mut links = Vec::new();
    let mut fenced = false;
    for (index, line) in text.lines().enumerate() {
        if line.trim_start().starts_with("```") {
            fenced = !fenced;
            continue;
        }
        if fenced {
            continue;
        }
        // Drop inline code spans: `[x](y)` inside backticks is not a link.
        let mut plain = String::new();
        let mut in_code = false;
        for ch in line.chars() {
            if ch == '`' {
                in_code = !in_code;
            } else if !in_code {
                plain.push(ch);
            }
        }
        let mut rest = plain.as_str();
        while let Some(at) = rest.find("](") {
            let tail = &rest[at + 2..];
            let Some(end) = tail.find(')') else { break };
            let target = tail[..end].trim();
            rest = &tail[end..];
            if target.is_empty()
                || target.starts_with('#')
                || target.starts_with('/')
                || target.contains("://")
                || target.starts_with("mailto:")
                || target.contains(char::is_whitespace)
            {
                continue;
            }
            let path = target.split(['#', '?']).next().unwrap_or("");
            if !path.is_empty() {
                links.push((index + 1, path.to_string()));
            }
        }
    }
    links
}

#[test]
fn docs_readme_indexes_every_doc_and_every_relative_link_resolves() {
    let repo = repo();
    let docs = repo.join("docs");
    let mut files = Vec::new();
    markdown_under(&docs, &mut files);
    files.sort();
    assert!(
        files.len() > 10,
        "found {} docs under {}",
        files.len(),
        docs.display()
    );

    let index_path = docs.join("README.md");
    let index = std::fs::read_to_string(&index_path).expect("docs/README.md exists");
    let indexed: BTreeSet<PathBuf> = relative_links(&index)
        .into_iter()
        .map(|(_, target)| normalize(&docs.join(target)))
        .collect();
    let unlisted: Vec<_> = files
        .iter()
        .filter(|file| **file != index_path)
        .filter(|file| !indexed.contains(&normalize(file)))
        .map(|file| file.strip_prefix(&repo).unwrap().display().to_string())
        .collect();
    assert!(
        unlisted.is_empty(),
        "docs/README.md does not list: {unlisted:#?}"
    );

    let mut checked = files.clone();
    checked.push(repo.join("README.md"));
    let mut broken = Vec::new();
    for file in &checked {
        let text = std::fs::read_to_string(file).unwrap();
        let base = file.parent().unwrap();
        for (line, target) in relative_links(&text) {
            let resolved = normalize(&base.join(&target));
            if !resolved.starts_with(normalize(&repo)) || !resolved.exists() {
                broken.push(format!(
                    "{}:{line}: {target}",
                    file.strip_prefix(&repo).unwrap().display()
                ));
            }
        }
    }
    assert!(broken.is_empty(), "broken relative links: {broken:#?}");
}

#[test]
fn link_extraction_ignores_code_and_absolute_targets() {
    let text = "see [a](a.md#x) and `[b](b.md)`\n```\n[c](c.md)\n```\n[d](https://x/y) [e](#top) [f](../f/)";
    let found: Vec<_> = relative_links(text).into_iter().map(|(_, t)| t).collect();
    assert_eq!(found, vec!["a.md".to_string(), "../f/".to_string()]);
}
