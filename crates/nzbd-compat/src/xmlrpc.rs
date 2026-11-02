//! XML-RPC endpoint (`/xmlrpc`) + `system.multicall`, sharing the same
//! method table as the JSON-RPC dialect. NZBGet clients send
//! string/int/i4/boolean/double/base64 scalars plus arrays and structs;
//! responses mirror JSON values back into XML-RPC types.

use crate::{dispatch_cluster, CompatState};
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use quick_xml::events::Event;
use quick_xml::Reader;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// parse
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct MethodCall {
    pub name: String,
    pub params: Value,
}

/// Parse an XML-RPC `<methodCall>`.
pub fn parse_call(xml: &str) -> Option<MethodCall> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut name = String::new();
    let mut params: Vec<Value> = Vec::new();
    let mut in_name = false;

    loop {
        match reader.read_event().ok()? {
            Event::Start(e) => match e.name().as_ref() {
                b"methodName" => in_name = true,
                b"value" => {
                    let v = parse_value(&mut reader)?;
                    params.push(v);
                }
                _ => {}
            },
            Event::Text(t) if in_name => {
                name = t.xml10_content().ok()?.into_owned();
                in_name = false;
            }
            Event::End(_) => {}
            Event::Eof => break,
            _ => {}
        }
    }
    if name.is_empty() {
        return None;
    }
    Some(MethodCall {
        name,
        params: Value::Array(params),
    })
}

/// Parse the contents of one `<value>` (reader positioned just after its
/// Start tag); consumes through the matching End tag.
fn parse_value(reader: &mut Reader<&[u8]>) -> Option<Value> {
    let mut result: Option<Value> = None;
    let mut bare_text: Option<String> = None;
    loop {
        match reader.read_event().ok()? {
            Event::Start(e) => match e.name().as_ref() {
                b"string" => result = Some(Value::String(read_text(reader, b"string")?)),
                b"int" | b"i4" | b"i8" => {
                    let tag = e.name().as_ref().to_vec();
                    let t = read_text_dyn(reader, &tag)?;
                    result = Some(json!(t.trim().parse::<i64>().unwrap_or(0)));
                }
                b"boolean" => {
                    let t = read_text(reader, b"boolean")?;
                    result = Some(Value::Bool(t.trim() == "1"));
                }
                b"double" => {
                    let t = read_text(reader, b"double")?;
                    result = Some(json!(t.trim().parse::<f64>().unwrap_or(0.0)));
                }
                b"base64" => {
                    // Delivered as the raw base64 string — the JSON dialect's
                    // append() decodes it exactly the same way.
                    let t = read_text(reader, b"base64")?;
                    result = Some(Value::String(t.split_whitespace().collect()));
                }
                b"nil" => result = Some(Value::Null),
                b"array" => result = Some(parse_array(reader)?),
                b"struct" => result = Some(parse_struct(reader)?),
                _ => {}
            },
            Event::Text(t) => {
                let chunk = t.xml10_content().ok()?.into_owned();
                bare_text.get_or_insert_with(String::new).push_str(&chunk);
            }
            Event::GeneralRef(r) => {
                bare_text
                    .get_or_insert_with(String::new)
                    .push_str(&resolve_ref(&r));
            }
            Event::End(e) if e.name().as_ref() == b"value" => {
                // <value>text</value> without a type tag = string.
                return Some(result.unwrap_or(Value::String(bare_text.unwrap_or_default())));
            }
            Event::End(_) => {}
            Event::Eof => return None,
            _ => {}
        }
    }
}

fn parse_array(reader: &mut Reader<&[u8]>) -> Option<Value> {
    let mut items = Vec::new();
    loop {
        match reader.read_event().ok()? {
            Event::Start(e) if e.name().as_ref() == b"value" => items.push(parse_value(reader)?),
            Event::End(e) if e.name().as_ref() == b"array" => return Some(Value::Array(items)),
            Event::Eof => return None,
            _ => {}
        }
    }
}

fn parse_struct(reader: &mut Reader<&[u8]>) -> Option<Value> {
    let mut map = serde_json::Map::new();
    let mut key = String::new();
    let mut in_name = false;
    loop {
        match reader.read_event().ok()? {
            Event::Start(e) => match e.name().as_ref() {
                b"name" => in_name = true,
                b"value" => {
                    let v = parse_value(reader)?;
                    map.insert(std::mem::take(&mut key), v);
                }
                _ => {}
            },
            Event::Text(t) if in_name => {
                key = t.xml10_content().ok()?.into_owned();
                in_name = false;
            }
            Event::End(e) if e.name().as_ref() == b"struct" => return Some(Value::Object(map)),
            Event::Eof => return None,
            _ => {}
        }
    }
}

/// Resolve `&name;` / `&#N;` general references (quick-xml emits them as
/// separate `GeneralRef` events).
fn resolve_ref(r: &quick_xml::events::BytesRef<'_>) -> String {
    if let Ok(Some(ch)) = r.resolve_char_ref() {
        return ch.to_string();
    }
    // Explicit slice type: with byte-string arms, `as_ref()` inference
    // latches onto the first arm's `&[u8; N]` on newer rustc.
    let name: &[u8] = r.as_ref();
    match name {
        b"lt" => "<".into(),
        b"gt" => ">".into(),
        b"amp" => "&".into(),
        b"apos" => "'".into(),
        b"quot" => "\"".into(),
        other => format!("&{};", String::from_utf8_lossy(other)),
    }
}

fn read_text(reader: &mut Reader<&[u8]>, end: &[u8]) -> Option<String> {
    read_text_dyn(reader, end)
}

fn read_text_dyn(reader: &mut Reader<&[u8]>, end: &[u8]) -> Option<String> {
    let mut out = String::new();
    loop {
        match reader.read_event().ok()? {
            Event::Text(t) => out.push_str(&t.xml10_content().ok()?),
            Event::GeneralRef(r) => out.push_str(&resolve_ref(&r)),
            Event::End(e) if e.name().as_ref() == end => return Some(out),
            Event::Eof => return None,
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// serialize
// ---------------------------------------------------------------------------

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// JSON value → XML-RPC `<value>` body.
pub fn to_xml(v: &Value, out: &mut String) {
    out.push_str("<value>");
    match v {
        Value::Null => out.push_str("<nil/>"),
        Value::Bool(b) => {
            out.push_str("<boolean>");
            out.push(if *b { '1' } else { '0' });
            out.push_str("</boolean>");
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                out.push_str(&format!("<int>{i}</int>"));
            } else {
                out.push_str(&format!("<double>{}</double>", n));
            }
        }
        Value::String(s) => {
            out.push_str("<string>");
            out.push_str(&xml_escape(s));
            out.push_str("</string>");
        }
        Value::Array(items) => {
            out.push_str("<array><data>");
            for item in items {
                to_xml(item, out);
            }
            out.push_str("</data></array>");
        }
        Value::Object(map) => {
            out.push_str("<struct>");
            for (k, val) in map {
                out.push_str("<member><name>");
                out.push_str(&xml_escape(k));
                out.push_str("</name>");
                to_xml(val, out);
                out.push_str("</member>");
            }
            out.push_str("</struct>");
        }
    }
    out.push_str("</value>");
}

fn response_ok(result: &Value) -> String {
    let mut body = String::with_capacity(256);
    body.push_str("<?xml version=\"1.0\"?><methodResponse><params><param>");
    to_xml(result, &mut body);
    body.push_str("</param></params></methodResponse>");
    body
}

fn response_fault(code: i64, message: &str) -> String {
    let mut body = String::with_capacity(256);
    body.push_str("<?xml version=\"1.0\"?><methodResponse><fault>");
    to_xml(
        &json!({ "faultCode": code, "faultString": message }),
        &mut body,
    );
    body.push_str("</fault></methodResponse>");
    body
}

// ---------------------------------------------------------------------------
// endpoint
// ---------------------------------------------------------------------------

pub async fn handle(
    State(state): State<CompatState>,
    headers: axum::http::HeaderMap,
    cluster_removal:Option<axum::Extension<nzbd_api::cluster_removal::ClusterRemovalContext>>,
    body: String,
) -> Response {
    let Some(call) = parse_call(&body) else {
        return xml_response(response_fault(4, "Parse error"));
    };
    crate::note_client(&state, &headers, &call.name);

    // system.multicall: params[0] = [{methodName, params}, …]
    if call.name == "system.multicall" {
        let calls = call
            .params
            .get(0)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut results = Vec::new();
        for c in calls {
            let name = c["methodName"].as_str().unwrap_or_default().to_string();
            let params = c.get("params").cloned().unwrap_or(Value::Array(vec![]));
            match dispatch_cluster(&state, &name, &params,cluster_removal.as_ref().map(|context|&context.0)).await {
                // Per the multicall spec, each success is a 1-element array.
                Ok(v) => results.push(Value::Array(vec![v])),
                Err((code, msg)) => results.push(json!({ "faultCode": code, "faultString": msg })),
            }
        }
        return xml_response(response_ok(&Value::Array(results)));
    }

    match dispatch_cluster(&state, &call.name, &call.params,cluster_removal.as_ref().map(|context|&context.0)).await {
        Ok(v) => xml_response(response_ok(&v)),
        Err((code, msg)) => xml_response(response_fault(code, msg)),
    }
}

fn xml_response(body: String) -> Response {
    ([(axum::http::header::CONTENT_TYPE, "text/xml")], body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_malformed_and_handles_edge_types() {
        assert!(parse_call("not xml at all").is_none());
        assert!(parse_call("<methodCall><params/></methodCall>").is_none()); // no method name
        assert!(parse_call("<methodCall><methodName>x</methodName><params><param><value><int>NaN</int></value></param></params></methodCall>").is_some());

        // double, i8, negative int, base64-ish string, empty array/struct
        let xml = r#"<methodCall><methodName>t</methodName><params>
  <param><value><double>2.5</double></value></param>
  <param><value><i8>9000000000</i8></value></param>
  <param><value><int>-3</int></value></param>
  <param><value><array><data></data></array></value></param>
  <param><value><struct></struct></value></param>
</params></methodCall>"#;
        let call = parse_call(xml).unwrap();
        assert_eq!(
            call.params,
            serde_json::json!([2.5, 9000000000i64, -3, [], {}])
        );

        // Serializer: null, float, escaping in keys, empty containers.
        let v = serde_json::json!({"a&b": null, "f": 1.25, "e": [], "s": {}});
        let mut out = String::new();
        to_xml(&v, &mut out);
        assert!(out.contains("<name>a&amp;b</name>"));
        assert!(out.contains("<double>1.25</double>"));
        assert!(
            out.contains("<array><data></data></array>") || out.contains("<array><data/></array>")
        );
    }

    #[test]
    fn parses_typical_call() {
        let xml = r#"<?xml version="1.0"?>
<methodCall><methodName>editqueue</methodName><params>
  <param><value><string>GroupPause</string></value></param>
  <param><value><string></string></value></param>
  <param><value><array><data>
    <value><int>4</int></value>
    <value><i4>7</i4></value>
  </data></array></value></param>
</params></methodCall>"#;
        let call = parse_call(xml).unwrap();
        assert_eq!(call.name, "editqueue");
        assert_eq!(call.params, serde_json::json!(["GroupPause", "", [4, 7]]));
    }

    #[test]
    fn parses_untyped_and_bool_and_struct() {
        let xml = r#"<methodCall><methodName>x</methodName><params>
  <param><value>bare string</value></param>
  <param><value><boolean>1</boolean></value></param>
  <param><value><struct>
    <member><name>Key</name><value><int>3</int></value></member>
  </struct></value></param>
</params></methodCall>"#;
        let call = parse_call(xml).unwrap();
        assert_eq!(
            call.params,
            serde_json::json!(["bare string", true, {"Key": 3}])
        );
    }

    #[test]
    fn serializes_nested_values() {
        let v = serde_json::json!({"Name": "a<b", "N": 5, "Ok": true, "List": [1, "x"]});
        let mut out = String::new();
        to_xml(&v, &mut out);
        assert!(out.contains("<name>Name</name><value><string>a&lt;b</string></value>"));
        assert!(out.contains("<int>5</int>"));
        assert!(out.contains("<boolean>1</boolean>"));
        assert!(out.contains("<array><data><value><int>1</int></value>"));
        // Round-trip through the parser.
        let xml = format!(
            "<methodCall><methodName>t</methodName><params><param>{out}</param></params></methodCall>"
        );
        let call = parse_call(&xml).unwrap();
        assert_eq!(call.params[0], v);
    }
}
