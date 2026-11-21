//! RSS 2.0 / Atom feed parsing (quick-xml event walk), tuned for indexer
//! feeds: newznab RSS with `<enclosure url=…>` and `<newznab:attr>` size,
//! plain RSS with `<link>`, and Atom entries.

use quick_xml::events::Event;
use quick_xml::Reader;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FeedItem {
    pub title: String,
    /// Download URL (enclosure beats link).
    pub url: String,
    /// Stable identity for dedup (guid/id, else the URL).
    pub guid: String,
    pub category: String,
    pub size: u64,
    pub age_days: u32,
}

fn resolve_ref(r: &quick_xml::events::BytesRef<'_>) -> String {
    if let Ok(Some(ch)) = r.resolve_char_ref() {
        return ch.to_string();
    }
    match &**r {
        "lt" => "<".into(),
        "gt" => ">".into(),
        "amp" => "&".into(),
        "apos" => "'".into(),
        "quot" => "\"".into(),
        other => format!("&{other};"),
    }
}

/// RFC 2822 pubDate → age in days relative to `now_unix` (best effort).
fn age_days(pub_date: &str, now_unix: i64) -> u32 {
    // "Tue, 15 Jul 2026 04:00:00 +0000" — parse day/month/year only.
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let parts: Vec<&str> = pub_date.split_whitespace().collect();
    // Find "<day> <Mon> <year>" anywhere in the string.
    for w in parts.windows(3) {
        if let (Ok(day), Some(month), Ok(year)) = (
            w[0].parse::<i64>(),
            MONTHS.iter().position(|m| w[1].starts_with(m)),
            w[2].parse::<i64>(),
        ) {
            // days-from-civil (Hinnant).
            let (y, m, d) = (year, month as i64 + 1, day);
            let y2 = if m <= 2 { y - 1 } else { y };
            let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
            let yoe = y2 - era * 400;
            let mp = (m + 9) % 12;
            let doy = (153 * mp + 2) / 5 + d - 1;
            let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
            let days = era * 146_097 + doe - 719_468;
            let age = (now_unix / 86_400) - days;
            return age.clamp(0, u32::MAX as i64) as u32;
        }
    }
    0
}

/// Parse an RSS 2.0 or Atom document into items.
pub fn parse_feed(xml: &str, now_unix: i64) -> Vec<FeedItem> {
    let mut reader = Reader::from_str(xml);
    // Keep raw whitespace: entity refs arrive as separate events, and
    // trimming per-chunk would eat the spaces around them ("TV > HD").
    // Assembled values are trimmed once at element end.
    let mut items = Vec::new();
    let mut cur: Option<FeedItem> = None;
    let mut pub_date = String::new();
    let mut text_target: Option<&'static str> = None;
    let mut text = String::new();

    while let Ok(ev) = reader.read_event() {
        match ev {
            Event::Start(e) => {
                let name = e.local_name();
                match name.as_ref() {
                    "item" | "entry" => {
                        cur = Some(FeedItem::default());
                        pub_date.clear();
                    }
                    "title" if cur.is_some() => text_target = Some("title"),
                    "link" if cur.is_some() => {
                        // Atom: <link href="…"/>; RSS: <link>text</link>.
                        let href = e.attributes().flatten().find_map(|a| {
                            (a.key.local_name().as_ref() == "href").then(|| a.value.into_owned())
                        });
                        match href {
                            Some(h) => {
                                if let Some(item) = cur.as_mut() {
                                    if item.url.is_empty() {
                                        item.url = h;
                                    }
                                }
                            }
                            None => text_target = Some("link"),
                        }
                    }
                    "guid" | "id" if cur.is_some() => text_target = Some("guid"),
                    "category" if cur.is_some() => text_target = Some("category"),
                    "pubDate" | "published" | "updated" if cur.is_some() => {
                        text_target = Some("date")
                    }
                    "size" if cur.is_some() => text_target = Some("size"),
                    _ => {}
                }
                text.clear();
            }
            Event::Empty(e) => {
                let name = e.local_name();
                let Some(item) = cur.as_mut() else { continue };
                match name.as_ref() {
                    "enclosure" => {
                        for a in e.attributes().flatten() {
                            match a.key.local_name().as_ref() {
                                "url" => item.url = a.value.into_owned(),
                                "length" => item.size = a.value.parse().unwrap_or(item.size),
                                _ => {}
                            }
                        }
                    }
                    // <newznab:attr name="size" value="…"/> and friends.
                    "attr" => {
                        let mut aname = String::new();
                        let mut avalue = String::new();
                        for a in e.attributes().flatten() {
                            match a.key.local_name().as_ref() {
                                "name" => aname = a.value.into_owned(),
                                "value" => avalue = a.value.into_owned(),
                                _ => {}
                            }
                        }
                        match aname.as_str() {
                            "size" => item.size = avalue.parse().unwrap_or(item.size),
                            "category" if item.category.is_empty() => item.category = avalue,
                            _ => {}
                        }
                    }
                    "link" => {
                        // Atom self-closing link.
                        if let Some(h) = e.attributes().flatten().find_map(|a| {
                            (a.key.local_name().as_ref() == "href").then(|| a.value.into_owned())
                        }) {
                            if item.url.is_empty() {
                                item.url = h;
                            }
                        }
                    }
                    _ => {}
                }
            }
            Event::Text(t) if text_target.is_some() => {
                text.push_str(&t.xml10_content());
            }
            Event::GeneralRef(r) if text_target.is_some() => {
                text.push_str(&resolve_ref(&r));
            }
            Event::CData(c) if text_target.is_some() => {
                text.push_str(&c);
            }
            Event::End(e) => {
                let name = e.local_name();
                if let (Some(target), Some(item)) = (text_target.take(), cur.as_mut()) {
                    let value = text.trim().to_string();
                    match target {
                        "title" => item.title = value,
                        "link" if item.url.is_empty() => {
                            item.url = value;
                        }
                        "guid" => item.guid = value,
                        "category" if item.category.is_empty() => {
                            item.category = value;
                        }
                        "date" => pub_date = value,
                        "size" => item.size = value.parse().unwrap_or(item.size),
                        _ => {}
                    }
                }
                if matches!(name.as_ref(), "item" | "entry") {
                    if let Some(mut item) = cur.take() {
                        if item.guid.is_empty() {
                            item.guid = item.url.clone();
                        }
                        item.age_days = age_days(&pub_date, now_unix);
                        if !item.title.is_empty() && !item.url.is_empty() {
                            items.push(item);
                        }
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;

    const RSS: &str = r#"<?xml version="1.0"?>
<rss version="2.0" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/">
 <channel><title>indexer</title>
  <item>
    <title>Great.Show.S01E01.1080p.WEB</title>
    <link>https://idx.example/details/1</link>
    <guid isPermaLink="false">abc-1</guid>
    <category>TV &gt; HD</category>
    <pubDate>Tue, 15 Jul 2026 04:00:00 +0000</pubDate>
    <enclosure url="https://idx.example/get/1.nzb" length="2147483648" type="application/x-nzb"/>
    <newznab:attr name="size" value="2147483648"/>
  </item>
  <item>
    <title>Small.Doc.2026</title>
    <link>https://idx.example/get/2.nzb</link>
    <guid>abc-2</guid>
  </item>
 </channel></rss>"#;

    #[test]
    fn parses_newznab_rss() {
        let now = 20_651 * 86_400; // 2026-07-17
        let items = parse_feed(RSS, now);
        assert_eq!(items.len(), 2);
        let a = &items[0];
        assert_eq!(a.title, "Great.Show.S01E01.1080p.WEB");
        assert_eq!(
            a.url, "https://idx.example/get/1.nzb",
            "enclosure beats link"
        );
        assert_eq!(a.guid, "abc-1");
        assert_eq!(a.category, "TV > HD");
        assert_eq!(a.size, 2_147_483_648);
        assert_eq!(a.age_days, 2);
        let b = &items[1];
        assert_eq!(b.url, "https://idx.example/get/2.nzb");
        assert_eq!(b.size, 0);
    }

    #[test]
    fn parses_atom() {
        let atom = r#"<feed xmlns="http://www.w3.org/2005/Atom">
          <entry><title>Atom.Release.720p</title>
            <link href="https://idx.example/a.nzb"/><id>atom-1</id>
            <updated>2026-07-16T00:00:00Z</updated></entry></feed>"#;
        let items = parse_feed(atom, 0);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "Atom.Release.720p");
        assert_eq!(items[0].url, "https://idx.example/a.nzb");
        assert_eq!(items[0].guid, "atom-1");
    }

    #[test]
    fn guid_falls_back_to_url() {
        let rss = r#"<rss><channel><item><title>t</title>
          <link>https://x/y.nzb</link></item></channel></rss>"#;
        let items = parse_feed(rss, 0);
        assert_eq!(items[0].guid, "https://x/y.nzb");
    }
}
