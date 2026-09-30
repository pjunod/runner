//! Streaming NZB parser.
//!
//! Event-driven (quick-xml), namespace-agnostic, tolerant of the messy NZBs
//! found in the wild: unordered segments, XML entities in subjects and
//! message-ids, DOCTYPE headers, missing optional attributes.
//!
//! Filename *deobfuscation* beyond the classic quoted-subject heuristic is a
//! phase-1 concern (see ARCHITECTURE.md §3.2 — nzbget's `Deobfuscation.cpp`);
//! this crate extracts, orders, and totals.

use quick_xml::events::Event;
use quick_xml::Reader;

#[derive(Debug, thiserror::Error)]
pub enum NzbError {
    #[error("xml error: {0}")]
    Xml(#[from] quick_xml::Error),
    #[error("not an NZB document (no <nzb> root)")]
    NotNzb,
    #[error("NZB contains no files")]
    Empty,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NzbMeta {
    pub title: Option<String>,
    pub password: Option<String>,
    pub category: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSegment {
    /// Message-id without angle brackets.
    pub message_id: String,
    pub number: u32,
    /// Encoded size from `bytes=` (advisory).
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedFile {
    pub subject: String,
    pub poster: Option<String>,
    pub date: Option<i64>,
    pub groups: Vec<String>,
    /// Sorted by part number, deduplicated.
    pub segments: Vec<ParsedSegment>,
}

impl ParsedFile {
    /// Total encoded size of all segments.
    pub fn encoded_size(&self) -> u64 {
        self.segments.iter().map(|s| s.bytes).sum()
    }

    /// The classic quoted-name heuristic: `subject "name.ext" yEnc (1/5)`.
    /// Falls back to the whole subject.
    pub fn filename_hint(&self) -> String {
        if let Some(open) = self.subject.find('"') {
            if let Some(close) = self.subject[open + 1..].find('"') {
                let name = self.subject[open + 1..open + 1 + close].trim();
                if !name.is_empty() {
                    return name.to_string();
                }
            }
        }
        // Bounded bracketed form: only a safe, extension-bearing component
        // before yEnc. The owner still confirms it against decoded metadata.
        if self.subject.len() <= 8192 {
            let prefix = self.subject.split("yEnc").next().unwrap_or("");
            let candidates: Vec<&str> = prefix
                .split('[')
                .skip(1)
                .filter_map(|part| part.split_once(']').map(|(name, _)| name))
                .filter(|name| {
                    name.len() <= 255
                        && name.contains('.')
                        && !name.contains(['/', '\\', ':', '\0'])
                        && *name != ".."
                })
                .collect();
            if candidates.len() == 1 {
                return candidates[0].to_string();
            }
        }
        self.subject.trim().to_string()
    }

    pub fn looks_like_par2(&self) -> bool {
        let name = self.filename_hint().to_ascii_lowercase();
        name.ends_with(".par2")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedNzb {
    pub meta: NzbMeta,
    pub files: Vec<ParsedFile>,
}

impl ParsedNzb {
    pub fn encoded_size(&self) -> u64 {
        self.files.iter().map(|f| f.encoded_size()).sum()
    }
}

pub fn parse(input: &[u8]) -> Result<ParsedNzb, NzbError> {
    let mut reader = Reader::from_reader(input);
    let mut buf = Vec::new();

    let mut nzb = ParsedNzb::default();
    let mut saw_root = false;

    let mut cur_file: Option<ParsedFile> = None;
    let mut cur_segment: Option<ParsedSegment> = None;
    let mut text_target: Option<TextTarget> = None;
    let mut meta_type: Option<String> = None;

    #[derive(PartialEq)]
    enum TextTarget {
        Group,
        Segment,
        Meta,
    }

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                let name = e.local_name();
                match name.as_ref() {
                    b"nzb" => saw_root = true,
                    b"file" => {
                        let mut f = ParsedFile::default();
                        for attr in e.attributes().flatten() {
                            let val = attr
                                .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                                .unwrap_or_default();
                            match attr.key.local_name().as_ref() {
                                b"subject" => f.subject = val.into_owned(),
                                b"poster" => f.poster = Some(val.into_owned()),
                                b"date" => f.date = val.trim().parse().ok(),
                                _ => {}
                            }
                        }
                        cur_file = Some(f);
                    }
                    b"group" => text_target = Some(TextTarget::Group),
                    b"segment" => {
                        let mut seg = ParsedSegment {
                            message_id: String::new(),
                            number: 0,
                            bytes: 0,
                        };
                        for attr in e.attributes().flatten() {
                            let val = attr
                                .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                                .unwrap_or_default();
                            match attr.key.local_name().as_ref() {
                                b"bytes" => seg.bytes = val.trim().parse().unwrap_or(0),
                                b"number" => seg.number = val.trim().parse().unwrap_or(0),
                                _ => {}
                            }
                        }
                        cur_segment = Some(seg);
                        text_target = Some(TextTarget::Segment);
                    }
                    b"meta" => {
                        meta_type = e.attributes().flatten().find_map(|a| {
                            (a.key.local_name().as_ref() == b"type").then(|| {
                                a.normalized_value(quick_xml::XmlVersion::Implicit1_0)
                                    .unwrap_or_default()
                                    .to_lowercase()
                            })
                        });
                        text_target = Some(TextTarget::Meta);
                    }
                    _ => {}
                }
            }
            Event::Text(t) => {
                if let Some(target) = &text_target {
                    // xml10_content(): charset-decode + entity-unescape
                    let text = t.xml10_content().unwrap_or_default();
                    let text = text.trim();
                    if text.is_empty() {
                        // ignore whitespace-only nodes
                    } else {
                        match target {
                            TextTarget::Group => {
                                if let Some(f) = cur_file.as_mut() {
                                    f.groups.push(text.to_string());
                                }
                            }
                            TextTarget::Segment => {
                                if let Some(s) = cur_segment.as_mut() {
                                    s.message_id.push_str(text);
                                }
                            }
                            TextTarget::Meta => {
                                let value = text.to_string();
                                match meta_type.as_deref() {
                                    Some("password") => nzb.meta.password = Some(value),
                                    Some("category") => nzb.meta.category = Some(value),
                                    Some("title") | Some("name") => nzb.meta.title = Some(value),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            Event::End(e) => match e.local_name().as_ref() {
                b"file" => {
                    if let Some(mut f) = cur_file.take() {
                        f.segments.sort_by_key(|s| s.number);
                        f.segments.dedup_by_key(|s| s.number);
                        if !f.segments.is_empty() {
                            nzb.files.push(f);
                        }
                    }
                }
                b"segment" => {
                    if let Some(mut s) = cur_segment.take() {
                        s.message_id = s
                            .message_id
                            .trim()
                            .trim_start_matches('<')
                            .trim_end_matches('>')
                            .to_string();
                        if !s.message_id.is_empty() {
                            if let Some(f) = cur_file.as_mut() {
                                f.segments.push(s);
                            }
                        }
                    }
                    text_target = None;
                }
                b"group" | b"meta" => text_target = None,
                _ => {}
            },
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    if !saw_root {
        return Err(NzbError::NotNzb);
    }
    if nzb.files.is_empty() {
        return Err(NzbError::Empty);
    }
    Ok(nzb)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <head>
    <meta type="password">s3cret</meta>
    <meta type="category">tv</meta>
  </head>
  <file poster="poster@example.com" date="1720000000" subject="Great &amp; Stuff [1/2] - &quot;archive.part1.rar&quot; yEnc (1/3)">
    <groups>
      <group>alt.binaries.test</group>
      <group>alt.binaries.misc</group>
    </groups>
    <segments>
      <segment bytes="716800" number="2">seg2@news.example</segment>
      <segment bytes="716800" number="1">&lt;seg1@news.example&gt;</segment>
      <segment bytes="358400" number="3">seg3@news.example</segment>
    </segments>
  </file>
  <file poster="p" date="1720000001" subject="&quot;archive.vol00+01.par2&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="51200" number="1">par@news.example</segment>
    </segments>
  </file>
</nzb>"#;

    #[test]
    fn parses_sample() {
        let nzb = parse(SAMPLE.as_bytes()).unwrap();
        assert_eq!(nzb.meta.password.as_deref(), Some("s3cret"));
        assert_eq!(nzb.meta.category.as_deref(), Some("tv"));
        assert_eq!(nzb.files.len(), 2);

        let f = &nzb.files[0];
        assert_eq!(
            f.subject,
            r#"Great & Stuff [1/2] - "archive.part1.rar" yEnc (1/3)"#
        );
        assert_eq!(f.groups, vec!["alt.binaries.test", "alt.binaries.misc"]);
        // segments sorted by number, brackets stripped, entities unescaped
        assert_eq!(
            f.segments.iter().map(|s| s.number).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(f.segments[0].message_id, "seg1@news.example");
        assert_eq!(f.encoded_size(), 716800 * 2 + 358400);
        assert_eq!(f.filename_hint(), "archive.part1.rar");
        assert!(!f.looks_like_par2());

        let p = &nzb.files[1];
        assert!(p.looks_like_par2());
        assert_eq!(p.filename_hint(), "archive.vol00+01.par2");

        assert_eq!(nzb.encoded_size(), 716800 * 2 + 358400 + 51200);
    }

    #[test]
    fn rejects_non_nzb() {
        assert!(matches!(parse(b"<html></html>"), Err(NzbError::NotNzb)));
        assert!(matches!(
            parse(br#"<nzb xmlns="x"></nzb>"#),
            Err(NzbError::Empty)
        ));
    }

    #[test]
    fn filename_hint_fallback() {
        let f = ParsedFile {
            subject: "no quotes here (1/1)".into(),
            ..Default::default()
        };
        assert_eq!(f.filename_hint(), "no quotes here (1/1)");
    }
}
