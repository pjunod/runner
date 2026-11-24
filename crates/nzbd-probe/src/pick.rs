//! Which file in a release is the one worth peeking at.
//!
//! A release is a media file, or an archive set holding one, plus par2,
//! nfo, samples and the like. The probe wants the first bytes of the media,
//! which is the first volume of the largest archive set or the largest
//! loose media file. Names come from the NZB subjects; an obfuscated name
//! carries no extension and is judged by size alone, and the bytes then
//! decide (see `lib.rs`).

use nzbd_nzb::ParsedFile;

/// What a file in the release is, as far as its name says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// A volume of a RAR set: the set's stem and this volume's position
    /// (0 = first).
    Rar { stem: String, volume: u32 },
    /// A media file by its extension.
    Media,
    /// A name that says nothing (obfuscated, or an unknown extension).
    Unknown,
    /// par2, nfo, sfv, images, subtitles: never the media.
    Skip,
}

const MEDIA: &[&str] = &[
    "mkv", "mp4", "m4v", "mov", "ts", "m2ts", "avi", "wmv", "webm",
];
const SKIP: &[&str] = &[
    "par2", "nfo", "sfv", "srr", "jpg", "jpeg", "png", "gif", "txt", "srt", "sub", "idx", "ass",
    "nzb", "url", "exe", "md5", "sha1",
];

/// Classify a file name.
pub fn classify(name: &str) -> Kind {
    let lower = name.to_ascii_lowercase();
    let Some((stem, ext)) = lower.rsplit_once('.') else {
        return Kind::Unknown;
    };
    if ext == "rar" {
        // name.part07.rar → volume 6; name.rar → volume 0 of an old-style set.
        if let Some((inner, part)) = stem.rsplit_once(".part") {
            if let Ok(n) = part.parse::<u32>() {
                return Kind::Rar {
                    stem: inner.to_string(),
                    volume: n.saturating_sub(1),
                };
            }
        }
        return Kind::Rar {
            stem: stem.to_string(),
            volume: 0,
        };
    }
    // Old-style continuation volumes: name.r00 is the second volume.
    if ext.len() == 3 && ext.starts_with('r') {
        if let Ok(n) = ext[1..].parse::<u32>() {
            return Kind::Rar {
                stem: stem.to_string(),
                volume: n + 1,
            };
        }
    }
    if MEDIA.contains(&ext) {
        return Kind::Media;
    }
    if SKIP.contains(&ext) {
        return Kind::Skip;
    }
    Kind::Unknown
}

/// The probe's candidates in the order to try them: the first volume of the
/// largest RAR set or the largest media file first, then unnamed files by
/// size. par2 and the other companions are never candidates.
pub fn candidates(files: &[ParsedFile]) -> Vec<usize> {
    #[derive(Debug)]
    struct Group {
        total: u64,
        first: usize,
        first_volume: u32,
    }
    let mut groups: Vec<(String, Group)> = Vec::new();
    let mut unknown: Vec<(u64, usize)> = Vec::new();
    for (i, f) in files.iter().enumerate() {
        if f.looks_like_par2() {
            continue;
        }
        let size = f.encoded_size();
        let (key, volume) = match classify(&f.filename_hint()) {
            Kind::Skip => continue,
            Kind::Unknown => {
                unknown.push((size, i));
                continue;
            }
            Kind::Media => (format!("media:{i}"), 0),
            Kind::Rar { stem, volume } => (format!("rar:{stem}"), volume),
        };
        match groups.iter_mut().find(|(k, _)| *k == key) {
            Some((_, g)) => {
                g.total += size;
                if volume < g.first_volume {
                    (g.first, g.first_volume) = (i, volume);
                }
            }
            None => groups.push((
                key,
                Group {
                    total: size,
                    first: i,
                    first_volume: volume,
                },
            )),
        }
    }
    groups.sort_by(|a, b| b.1.total.cmp(&a.1.total).then(a.1.first.cmp(&b.1.first)));
    unknown.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut out: Vec<usize> = groups.iter().map(|(_, g)| g.first).collect();
    // Unnamed files compete on size with what the names identified: a large
    // obfuscated file outranks a small named sample.
    let biggest_named = groups.first().map(|(_, g)| g.total).unwrap_or(0);
    let (big, small): (Vec<_>, Vec<_>) = unknown.into_iter().partition(|(s, _)| *s > biggest_named);
    let mut ordered: Vec<usize> = big.into_iter().map(|(_, i)| i).collect();
    ordered.append(&mut out);
    ordered.extend(small.into_iter().map(|(_, i)| i));
    ordered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_volumes_media_and_companions() {
        assert_eq!(
            classify("Title.2021.part01.rar"),
            Kind::Rar {
                stem: "title.2021".into(),
                volume: 0
            }
        );
        assert_eq!(
            classify("Title.2021.part12.rar"),
            Kind::Rar {
                stem: "title.2021".into(),
                volume: 11
            }
        );
        assert_eq!(
            classify("title.rar"),
            Kind::Rar {
                stem: "title".into(),
                volume: 0
            }
        );
        assert_eq!(
            classify("title.r00"),
            Kind::Rar {
                stem: "title".into(),
                volume: 1
            }
        );
        assert_eq!(classify("Title.2021.2160p.mkv"), Kind::Media);
        assert_eq!(classify("title.vol03+04.par2"), Kind::Skip);
        assert_eq!(classify("title.nfo"), Kind::Skip);
        assert_eq!(classify("a8f3c1e9d04b77e2c519f0a6"), Kind::Unknown);
    }
}
