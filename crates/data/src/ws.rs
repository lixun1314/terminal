//! A websocket client: tokio-tungstenite on the desktop, the browser's `WebSocket` on the web.

use anyhow::Result;

#[cfg(not(target_arch = "wasm32"))]
pub use native::Socket;
#[cfg(target_arch = "wasm32")]
pub use web::Socket;

/// A data message. Control frames stay inside the socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Text(String),
    Binary(Vec<u8>),
}

impl Socket {
    pub async fn send(&mut self, text: String) -> Result<()> {
        self.send_message(Message::Text(text)).await
    }

    /// The next text message, skipping binary ones; `None` once the server closes the connection.
    pub async fn next(&mut self) -> Option<Result<String>> {
        loop {
            match self.next_message().await? {
                Ok(Message::Text(t)) => return Some(Ok(t)),
                Ok(Message::Binary(_)) => {}
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use anyhow::{Context, Result, bail};
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message as WsMessage;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    use super::Message;

    pub struct Socket(WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>);

    impl Socket {
        pub async fn connect(url: &str) -> Result<Self> {
            Self::connect_with_headers(url, &[]).await
        }

        /// Connect with extra request headers (TradingView requires `Origin`, and a session
        /// `Cookie` unlocks realtime entitlements). Uses the configured HTTP proxy as a
        /// CONNECT tunnel when one is set: the websocket stack does not read OS proxy settings.
        pub async fn connect_with_headers(url: &str, headers: &[(&str, &str)]) -> Result<Self> {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;
            use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
            let mut req = url.into_client_request().with_context(|| format!("websocket {url}"))?;
            for (k, v) in headers {
                req.headers_mut().insert(
                    HeaderName::from_bytes(k.as_bytes()).with_context(|| format!("header name {k}"))?,
                    HeaderValue::from_str(v).with_context(|| format!("header value {k}"))?,
                );
            }
            let host = req.uri().host().unwrap_or_default().to_string();
            let port = req.uri().port_u16().unwrap_or(443);
            // HTTP CONNECT tunnel through the configured proxy, when there is one.
            if let Some(p) = tr_core::http::proxy_url()
                && let Ok(proxy) = reqwest::Url::parse(&p)
                && proxy.scheme().starts_with("http")
            {
                let phost = proxy.host_str().unwrap_or("127.0.0.1").to_string();
                let pport = proxy.port().unwrap_or(8080);
                let mut tcp = tokio::net::TcpStream::connect((phost.as_str(), pport))
                    .await
                    .with_context(|| format!("proxy {phost}:{pport}"))?;
                let reqline = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n");
                tcp.write_all(reqline.as_bytes()).await?;
                let mut buf = Vec::new();
                let mut byte = [0u8; 1];
                while !buf.ends_with(b"\r\n\r\n") && buf.len() < 8192 {
                    if tcp.read(&mut byte).await? == 0 {
                        break;
                    }
                    buf.push(byte[0]);
                }
                if !buf.starts_with(b"HTTP/1.1 200") {
                    bail!("proxy CONNECT {host}:{port} rejected");
                }
                let (ws, _) = tokio_tungstenite::client_async_tls_with_config(req, tcp, None, None)
                    .await
                    .with_context(|| format!("websocket {url} via proxy"))?;
                return Ok(Self(ws));
            }
            let (ws, _) = tokio_tungstenite::connect_async(req).await.with_context(|| format!("websocket {url}"))?;
            Ok(Self(ws))
        }

        pub async fn send_message(&mut self, m: Message) -> Result<()> {
            let m = match m {
                Message::Text(t) => WsMessage::Text(t.into()),
                Message::Binary(b) => WsMessage::Binary(b.into()),
            };
            Ok(self.0.send(m).await?)
        }

        /// The next data message; `None` once the server closes the connection.
        pub async fn next_message(&mut self) -> Option<Result<Message>> {
            while let Some(msg) = self.0.next().await {
                match msg {
                    Ok(WsMessage::Text(t)) => return Some(Ok(Message::Text(t.as_str().to_owned()))),
                    Ok(WsMessage::Binary(b)) => return Some(Ok(Message::Binary(b.into()))),
                    Ok(WsMessage::Close(_)) => return None,
                    Ok(_) => {}
                    Err(e) => return Some(Err(e.into())),
                }
            }
            None
        }

        /// Send a close frame.
        pub async fn close(&mut self) {
            let _ = self.0.close(None).await;
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod web {
    use anyhow::{Result, anyhow, bail};
    use tokio::sync::mpsc;
    use wasm_bindgen::JsCast;
    use wasm_bindgen::prelude::*;
    use web_sys::{BinaryType, CloseEvent, MessageEvent, WebSocket};

    use super::Message;

    enum Event {
        Open,
        Message(Message),
        Error,
        Closed,
    }

    /// Browser websocket; its callbacks feed a channel. Closed when dropped.
    pub struct Socket {
        ws: WebSocket,
        rx: mpsc::UnboundedReceiver<Event>,
        _on_open: Closure<dyn FnMut()>,
        _on_message: Closure<dyn FnMut(MessageEvent)>,
        _on_error: Closure<dyn FnMut()>,
        _on_close: Closure<dyn FnMut(CloseEvent)>,
    }

    impl Socket {
        pub async fn connect(url: &str) -> Result<Self> {
            let ws = WebSocket::new(url).map_err(|e| anyhow!("websocket {url}: {e:?}"))?;
            // Binary frames arrive as ArrayBuffers rather than Blobs, which can only be read asynchronously.
            ws.set_binary_type(BinaryType::Arraybuffer);
            let (tx, rx) = mpsc::unbounded_channel();
            let t = tx.clone();
            let on_open = Closure::<dyn FnMut()>::new(move || {
                let _ = t.send(Event::Open);
            });
            let t = tx.clone();
            let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
                let data = e.data();
                let msg = if let Some(text) = data.as_string() {
                    Message::Text(text)
                } else if let Some(buf) = data.dyn_ref::<js_sys::ArrayBuffer>() {
                    Message::Binary(js_sys::Uint8Array::new(buf).to_vec())
                } else {
                    return;
                };
                let _ = t.send(Event::Message(msg));
            });
            let t = tx.clone();
            let on_error = Closure::<dyn FnMut()>::new(move || {
                let _ = t.send(Event::Error);
            });
            let on_close = Closure::<dyn FnMut(CloseEvent)>::new(move |_| {
                let _ = tx.send(Event::Closed);
            });
            ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
            ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
            ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
            ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));
            let mut socket =
                Self { ws, rx, _on_open: on_open, _on_message: on_message, _on_error: on_error, _on_close: on_close };
            match socket.rx.recv().await {
                Some(Event::Open) => Ok(socket),
                _ => bail!("websocket {url}: connection failed"),
            }
        }

        pub async fn send_message(&mut self, m: Message) -> Result<()> {
            match m {
                Message::Text(t) => self.ws.send_with_str(&t),
                Message::Binary(b) => self.ws.send_with_u8_array(&b),
            }
            .map_err(|e| anyhow!("websocket send: {e:?}"))
        }

        /// The next data message; `None` once the server closes the connection.
        pub async fn next_message(&mut self) -> Option<Result<Message>> {
            match self.rx.recv().await? {
                Event::Message(m) => Some(Ok(m)),
                Event::Error => Some(Err(anyhow!("websocket error"))),
                Event::Open | Event::Closed => None,
            }
        }

        /// Send a close frame.
        pub async fn close(&mut self) {
            let _ = self.ws.close();
        }
    }

    impl Drop for Socket {
        fn drop(&mut self) {
            // Detach the callbacks before their closures are freed.
            self.ws.set_onopen(None);
            self.ws.set_onmessage(None);
            self.ws.set_onerror(None);
            self.ws.set_onclose(None);
            let _ = self.ws.close();
        }
    }
}
