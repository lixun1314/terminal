//! Outbound HTTP setup shared by the data sources: an optional proxy for endpoints
//! that geo-block or throttle direct connections (Yahoo Finance, Senate eFD).
//!
//! Resolution order:
//! 1. `TR_PROXY` (`http://host:port`, `socks5://host:port`, or `off` to force direct),
//! 2. `proxy.txt` in the data directory (first non-empty, non-comment line),
//! 3. the standard `HTTPS_PROXY`/`HTTP_PROXY`/`ALL_PROXY` variables reqwest reads itself.
//!
//! A GUI launch inherits no shell environment, so the file is the durable way to
//! configure a proxy; `TR_PROXY` wins for one-off runs.

/// A configured proxy: unset (reqwest's environment defaults), forced off, or a URL.
#[derive(Debug, PartialEq)]
enum Proxy {
    Auto,
    Off,
    Url(String),
}

fn parse(value: &str) -> Proxy {
    let v = value.trim();
    if v.is_empty() || matches!(v.to_ascii_lowercase().as_str(), "off" | "none" | "direct" | "0") {
        Proxy::Off
    } else {
        Proxy::Url(v.to_string())
    }
}

/// The first non-empty, non-comment line of a `proxy.txt`.
fn from_file(text: &str) -> Option<Proxy> {
    text.lines().map(str::trim).find(|l| !l.is_empty() && !l.starts_with('#')).map(parse)
}

/// The desktop data directory (the same place the worker keeps its SQLite stores).
#[cfg(not(target_arch = "wasm32"))]
fn data_dir() -> std::path::PathBuf {
    if let Some(d) = std::env::var_os("TR_DATA_DIR") {
        return d.into();
    }
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from).unwrap_or_else(|| ".".into());
    if cfg!(target_os = "macos") {
        home.join("Library/Application Support/terminal")
    } else {
        std::env::var_os("XDG_DATA_HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| home.join(".local/share"))
            .join("terminal")
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn configured() -> Proxy {
    if let Ok(v) = std::env::var("TR_PROXY") {
        return parse(&v);
    }
    if let Ok(text) = std::fs::read_to_string(data_dir().join("proxy.txt"))
        && let Some(p) = from_file(&text)
    {
        return p;
    }
    Proxy::Auto
}

/// A [`reqwest::ClientBuilder`] with the configured proxy applied. The browser build
/// has no proxy choice; on the desktop the standard environment variables still apply
/// when nothing is configured here.
pub fn client_builder() -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder();
    #[cfg(not(target_arch = "wasm32"))]
    let builder = match configured() {
        Proxy::Auto => builder,
        Proxy::Off => builder.no_proxy(),
        Proxy::Url(url) => match reqwest::Proxy::all(&url) {
            Ok(p) => builder.proxy(p.no_proxy(reqwest::NoProxy::from_env())),
            Err(e) => {
                tracing::warn!("ignoring invalid proxy {url:?}: {e}");
                builder
            }
        },
    };
    builder
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_proxy_values() {
        assert_eq!(parse("http://127.0.0.1:7890"), Proxy::Url("http://127.0.0.1:7890".into()));
        assert_eq!(parse("  socks5://127.0.0.1:7891 "), Proxy::Url("socks5://127.0.0.1:7891".into()));
        for off in ["", "off", "OFF", "none", "direct", "0"] {
            assert_eq!(parse(off), Proxy::Off);
        }
    }

    #[test]
    fn reads_first_meaningful_line() {
        assert_eq!(
            from_file("# local clash\n\nhttp://127.0.0.1:7890\n"),
            Some(Proxy::Url("http://127.0.0.1:7890".into()))
        );
        assert_eq!(from_file("off\n"), Some(Proxy::Off));
        assert_eq!(from_file("# only comments\n\n"), None);
    }
}
