//! Attachment previews: one thumbnail per image row, and the URL a click opens.
//!
//! The browser client renders `<a href={url}><img src={url}/></a>` and gets the
//! fetch, the decode and the click target from the platform. A native window has
//! none of that, so this module supplies the three pieces the desktop client
//! needs, and nothing more:
//!
//! | Piece | Here |
//! | :-- | :-- |
//! | The absolute URL | [`Previews::resolve`], off a base derived once from the socket URL |
//! | The bytes | [`fetch`] — one HTTP/1.1 GET over `async-net` |
//! | The decode | gpui's, through [`gpui::Image::from_bytes`] |
//!
//! # No TLS, and no HTTP dependency
//!
//! `transport.rs` refuses `wss://` because the Musubi path links no TLS stack,
//! and a preview fetcher that pulled `reqwest` in would undo that in one line.
//! So the fetch is the same plain TCP the websocket already speaks: `async-net`
//! for the socket, thirty lines for the request and the status check. An
//! `https://` attachment URL is therefore not fetchable — it degrades to the
//! chip, and the click still opens it, because the browser owns the TLS this
//! client does not link.
//!
//! gpui's own `img("http://…")` path was the alternative. It is real —
//! `ImageSource::Resource(Resource::Uri)` fetches through
//! `App::http_client()` — but the default client is `NullHttpClient`, which
//! fails every request, and the clients that do not are TLS-bearing. It also
//! starts the fetch from the render path, which is exactly the shape this
//! module exists to avoid.
//!
//! # No image dependency either
//!
//! [`gpui::Image::from_bytes`] takes a [`gpui::ImageFormat`] and the raw bytes
//! and hands the decode to gpui's asset system, which already depends on
//! `image`. What that costs here is one enum lookup —
//! [`gpui::ImageFormat::from_mime_type`] against the attachment's declared
//! `content_type` — and what it buys is that the crate adds no decoder, no
//! codec features and no second copy of `image` to the build.

use std::collections::HashMap;
use std::sync::Arc;

use futures::{AsyncReadExt as _, AsyncWriteExt as _};
use gpui::{Image, ImageFormat, SharedString};

use crate::generated::chat_room::AttachmentState;
use crate::transport::host_port;

/// The port an `http://` URL without one implies.
const HTTP_PORT: u16 = 80;

/// The most one preview will read off the socket.
///
/// The store declares `max_file_size: 2_000_000`, so this is slack rather than a
/// policy: it is the backstop that keeps a server which never closes the
/// connection from growing the buffer without end.
const MAX_PREVIEW_BYTES: u64 = 8 * 1024 * 1024;

/// What one attachment URL has come to.
///
/// There is no "retry" arm on purpose (requirement of the render path, not of
/// the network): [`Failed`](Preview::Failed) is terminal, so a chip that
/// degraded stays degraded instead of dialing the server again on every redraw.
#[derive(Clone)]
enum Preview {
    /// The fetch is in flight. Renders as the chip until it lands.
    Loading,
    /// Fetched. The decode itself is gpui's, and happens the first time the
    /// element paints.
    Ready(Arc<Image>),
    /// Not an image, not reachable, not a `200`, or not decodable.
    Failed,
}

/// One attachment, as the render path needs it.
#[derive(Clone)]
struct Entry {
    /// The absolute URL a click opens. Built once, cloned per frame.
    link: SharedString,
    preview: Preview,
}

/// The per-URL preview cache, owned by the window entity.
///
/// Two rules hold it together:
///
/// * **Nothing here starts work.** [`resolve`](Self::resolve) is a map lookup
///   and two `Arc` clones; [`begin`](Self::begin) is the only mutator that can
///   report a fetch, and it is called from a state subscription rather than
///   from `render`.
/// * **One fetch per URL, ever.** `begin` reports a URL exactly once, so a scan
///   that runs on every collection transaction is idempotent.
#[derive(Clone)]
pub struct Previews {
    /// `http://host:port`, derived once from the socket URL. `None` when the
    /// socket URL is not a `ws://` one, which leaves relative URLs unresolvable
    /// and every preview degraded.
    base: Option<SharedString>,
    entries: HashMap<String, Entry>,
}

impl Previews {
    /// Derives the origin the attachments are served from.
    ///
    /// The example serves `/attachments/:id` off the same host and port as
    /// `/socket`, so the socket URL the app was started with is the whole
    /// configuration. This runs once, at window construction — never per frame,
    /// and never out of `MUSUBI_URL` deep in the render path.
    pub fn new(socket_url: &str) -> Self {
        Self {
            base: http_base(socket_url),
            entries: HashMap::new(),
        }
    }

    /// The render path's whole read: the URL a click opens, and the image to
    /// draw when there is one.
    ///
    /// The `None` arm covers one frame's worth of race. The scan that fills the
    /// map and the list splice that paints the row are two foreground tasks, so
    /// a row can reach the screen before its entry exists; building the link
    /// here costs one allocation for that frame and none afterwards.
    pub fn resolve(&self, url: &str) -> (SharedString, Option<Arc<Image>>) {
        let Some(entry) = self.entries.get(url) else {
            return (absolute(self.base.as_ref(), url), None);
        };

        let image = match &entry.preview {
            Preview::Ready(image) => Some(Arc::clone(image)),
            Preview::Loading | Preview::Failed => None,
        };

        (entry.link.clone(), image)
    }

    /// Records an attachment, and reports the fetch that sighting starts.
    ///
    /// `Some((link, format))` means "nobody has fetched this URL yet and it
    /// claims to be an image". Every other case — a URL already known, a
    /// `content_type` gpui cannot decode — is `None`, which is what makes a
    /// non-image cost nothing but a map entry.
    pub fn begin(&mut self, attachment: &AttachmentState) -> Option<(SharedString, ImageFormat)> {
        if self.entries.contains_key(&attachment.url) {
            return None;
        }

        let link = absolute(self.base.as_ref(), &attachment.url);
        let format = preview_format(&attachment.content_type);

        self.entries.insert(
            attachment.url.clone(),
            Entry {
                link: link.clone(),
                // A type gpui has no decoder for is decided here, once, and
                // never fetched: the chip is the answer for a text file.
                preview: match format {
                    Some(_) => Preview::Loading,
                    None => Preview::Failed,
                },
            },
        );

        Some((link, format?))
    }

    /// Settles the entry a [`begin`](Self::begin) opened.
    ///
    /// A failure is logged and forgotten: the chip is a complete rendering of an
    /// attachment, so there is nothing to tell the user and nothing to retry.
    pub fn finish(&mut self, url: &str, fetched: Result<Arc<Image>, String>) {
        let Some(entry) = self.entries.get_mut(url) else {
            return;
        };

        entry.preview = match fetched {
            Ok(image) => Preview::Ready(image),
            Err(reason) => {
                tracing::debug!(url, reason = %reason, "no attachment preview; showing the chip");

                Preview::Failed
            }
        };
    }

    /// Test seam: puts bytes in the cache as though a fetch had landed.
    ///
    /// Unconditional, unlike [`begin`](Self::begin) plus
    /// [`finish`](Self::finish): a UI test seeds an entry the scan has already
    /// opened, so this has to overwrite rather than refuse. There is no live
    /// server in a `#[gpui::test]`, and this is the only way past that.
    #[cfg(test)]
    pub fn seed(&mut self, attachment: &AttachmentState, bytes: Vec<u8>) {
        let Some(format) = preview_format(&attachment.content_type) else {
            return;
        };

        self.entries.insert(
            attachment.url.clone(),
            Entry {
                link: absolute(self.base.as_ref(), &attachment.url),
                preview: Preview::Ready(Arc::new(Image::from_bytes(format, bytes))),
            },
        );
    }
}

/// `ws://host[:port]/socket` → `http://host:port`.
///
/// The port is defaulted and an IPv6 literal keeps its brackets, because
/// [`host_port`] is the same parser the connector dials with. `wss://` yields
/// `None` rather than a guess: this client links no TLS stack, so a base built
/// from one could never be fetched.
fn http_base(socket_url: &str) -> Option<SharedString> {
    let rest = socket_url.strip_prefix("ws://")?;

    Some(format!("http://{}", host_port(rest, HTTP_PORT)?).into())
}

/// The URL a click opens.
///
/// An attachment URL that is already absolute is passed through untouched — the
/// server is free to hand out a CDN link, and this client is not the one to
/// rewrite it. Anything else is server-relative (`/attachments/att-1`) and is
/// joined onto the origin the socket was dialed on.
fn absolute(base: Option<&SharedString>, url: &str) -> SharedString {
    if url.starts_with("http://") || url.starts_with("https://") {
        return url.to_owned().into();
    }

    match base {
        Some(base) if url.starts_with('/') => format!("{base}{url}").into(),
        Some(base) => format!("{base}/{url}").into(),
        // No origin to join onto. The raw value is handed on rather than
        // guessed at; the click will fail, and the chip is already correct.
        None => url.to_owned().into(),
    }
}

/// The decision to fetch, in one place: gpui's decoder for a `content_type`, or
/// `None` for anything that is not an image.
///
/// [`ImageFormat::from_mime_type`] is gpui's own table and matches exactly, so
/// the parameters and the case a `Content-Type` may legally carry
/// (`IMAGE/PNG; charset=binary`) are stripped first.
fn preview_format(content_type: &str) -> Option<ImageFormat> {
    let essence = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    ImageFormat::from_mime_type(&essence)
}

/// One HTTP/1.1 GET, over the same plain TCP the websocket speaks.
///
/// `Connection: close` is what keeps this short: the response body is then
/// delimited by the end of the stream, so no `Content-Length` bookkeeping and no
/// chunked decoder is needed — and a server that answers chunked anyway is
/// reported as a failure rather than rendered as garbage.
///
/// Runs on `cx.background_executor()`, never on the UI thread.
pub async fn fetch(link: &str) -> Result<Vec<u8>, String> {
    let (authority, path) = target(link)?;

    let mut stream = async_net::TcpStream::connect(authority.as_str())
        .await
        .map_err(|error| format!("connect {authority}: {error}"))?;

    let request = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");

    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| format!("GET {path}: {error}"))?;

    let mut response = Vec::new();

    stream
        .take(MAX_PREVIEW_BYTES)
        .read_to_end(&mut response)
        .await
        .map_err(|error| format!("reading {path}: {error}"))?;

    body(&response).map(<[u8]>::to_vec)
}

/// Splits an absolute `http://` URL into the TCP target and the request target.
fn target(link: &str) -> Result<(String, String), String> {
    let rest = link
        .strip_prefix("http://")
        .ok_or_else(|| format!("only http:// can be fetched without a TLS stack: {link}"))?;

    let authority = host_port(rest, HTTP_PORT).ok_or_else(|| format!("no authority in {link}"))?;

    // Everything from the first `/`, `?` or `#`; a URL with no path at all asks
    // for the root.
    let path = match rest.find(['/', '?', '#']) {
        Some(start) if rest[start..].starts_with('/') => rest[start..].to_owned(),
        Some(start) => format!("/{}", &rest[start..]),
        None => "/".to_owned(),
    };

    Ok((authority, path))
}

/// The body of a `200` response, or why there is none.
///
/// Only the two headers that decide whether the bytes are usable are read: the
/// status line, and `Transfer-Encoding`. Everything else a real client would do
/// with a response — redirects, caching, content negotiation — is out of scope
/// for a thumbnail the app can simply not draw.
fn body(response: &[u8]) -> Result<&[u8], String> {
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| "the response carried no header block".to_owned())?;

    let (head, body) = response.split_at(split + 4);
    let head = String::from_utf8_lossy(head);
    let mut lines = head.lines();

    // `HTTP/1.1 200 OK` — the code is the second token.
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| "the response carried no status line".to_owned())?;

    if status != "200" {
        return Err(format!("the server answered {status}"));
    }

    let chunked = lines.any(|line| {
        let (name, value) = line.split_once(':').unwrap_or_default();

        name.eq_ignore_ascii_case("transfer-encoding")
            && value.to_ascii_lowercase().contains("chunked")
    });

    if chunked {
        return Err("the response is chunked, which this reader does not decode".to_owned());
    }

    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attachment(content_type: &str, url: &str) -> AttachmentState {
        AttachmentState {
            name: "shot.png".to_owned(),
            content_type: content_type.to_owned(),
            size: 12,
            url: url.to_owned(),
        }
    }

    #[test]
    fn derives_the_http_origin_from_the_socket_url() {
        assert_eq!(
            http_base("ws://127.0.0.1:4002/socket").unwrap(),
            "http://127.0.0.1:4002"
        );
    }

    #[test]
    fn defaults_the_port_and_keeps_a_bracketed_ipv6_literal() {
        assert_eq!(
            http_base("ws://example.test/socket").unwrap(),
            "http://example.test:80"
        );
        assert_eq!(http_base("ws://[::1]/socket").unwrap(), "http://[::1]:80");
        assert_eq!(
            http_base("ws://[::1]:4002/socket").unwrap(),
            "http://[::1]:4002"
        );
    }

    /// `MUSUBI_URL` is read once in `main.rs` and handed straight to the window,
    /// so the only shapes this has to cover are the ones that variable can hold.
    #[test]
    fn refuses_a_socket_url_it_could_not_fetch_from() {
        assert!(http_base("wss://example.test/socket").is_none());
        assert!(http_base("ws:///socket").is_none());
    }

    #[test]
    fn joins_a_server_relative_attachment_url_onto_the_origin() {
        let base = http_base("ws://127.0.0.1:4002/socket");

        assert_eq!(
            absolute(base.as_ref(), "/attachments/att-1"),
            "http://127.0.0.1:4002/attachments/att-1"
        );
        assert_eq!(
            absolute(base.as_ref(), "attachments/att-1"),
            "http://127.0.0.1:4002/attachments/att-1"
        );
    }

    #[test]
    fn passes_an_absolute_attachment_url_through() {
        let base = http_base("ws://127.0.0.1:4002/socket");

        assert_eq!(
            absolute(base.as_ref(), "http://cdn.test/att-1.png"),
            "http://cdn.test/att-1.png"
        );
        assert_eq!(
            absolute(base.as_ref(), "https://cdn.test/att-1.png"),
            "https://cdn.test/att-1.png"
        );
        // No origin: the raw value, not a guess.
        assert_eq!(absolute(None, "/attachments/att-1"), "/attachments/att-1");
    }

    #[test]
    fn maps_an_image_content_type_onto_a_gpui_decoder() {
        assert_eq!(preview_format("image/png"), Some(ImageFormat::Png));
        assert_eq!(preview_format("image/jpeg"), Some(ImageFormat::Jpeg));
        assert_eq!(preview_format("image/gif"), Some(ImageFormat::Gif));
        assert_eq!(
            preview_format("IMAGE/PNG; charset=binary"),
            Some(ImageFormat::Png)
        );
    }

    #[test]
    fn refuses_a_content_type_that_is_not_an_image() {
        assert_eq!(preview_format("text/plain"), None);
        assert_eq!(preview_format("application/octet-stream"), None);
        assert_eq!(preview_format(""), None);
    }

    #[test]
    fn fetches_an_image_url_once_and_never_a_text_one() {
        let mut previews = Previews::new("ws://127.0.0.1:4002/socket");
        let image = attachment("image/png", "/attachments/att-1");

        let (link, format) = previews.begin(&image).expect("an image is fetched");

        assert_eq!(link, "http://127.0.0.1:4002/attachments/att-1");
        assert_eq!(format, ImageFormat::Png);
        assert!(previews.begin(&image).is_none(), "one fetch per URL");

        let text = attachment("text/plain", "/attachments/att-2");

        assert!(
            previews.begin(&text).is_none(),
            "a text file is not fetched"
        );
        assert!(previews.begin(&text).is_none());
    }

    /// A failed URL is remembered as failed, so the render path cannot start the
    /// fetch again.
    #[test]
    fn a_failure_settles_the_entry_without_reopening_it() {
        let mut previews = Previews::new("ws://127.0.0.1:4002/socket");
        let image = attachment("image/png", "/attachments/att-1");

        previews.begin(&image).expect("an image is fetched");
        previews.finish(&image.url, Err("connection refused".to_owned()));

        assert!(previews.resolve(&image.url).1.is_none());
        assert!(previews.begin(&image).is_none());
    }

    #[test]
    fn splits_a_link_into_a_tcp_target_and_a_request_target() {
        assert_eq!(
            target("http://127.0.0.1:4002/attachments/att-1").unwrap(),
            ("127.0.0.1:4002".to_owned(), "/attachments/att-1".to_owned())
        );
        assert_eq!(
            target("http://example.test").unwrap(),
            ("example.test:80".to_owned(), "/".to_owned())
        );
        assert!(target("https://example.test/att-1.png").is_err());
    }

    #[test]
    fn takes_the_body_of_a_200_and_nothing_else() {
        assert_eq!(
            body(b"HTTP/1.1 200 OK\r\nContent-Type: image/png\r\n\r\nPNG").unwrap(),
            b"PNG"
        );
        assert!(body(b"HTTP/1.1 404 Not Found\r\n\r\nnope").is_err());
        assert!(body(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nPNG").is_err());
        assert!(body(b"HTTP/1.1 200 OK\r\n").is_err());
    }

    /// Answers one request on a loopback port and hands back what it read.
    ///
    /// Not a live server — a `TcpListener` this test owns, bound to port 0 and
    /// closed when the thread ends. It is the only way to check the bytes
    /// [`fetch`] actually writes, which no pure test of [`target`] can.
    fn one_shot_server(response: &'static [u8]) -> (String, std::thread::JoinHandle<String>) {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port");
        let authority = listener
            .local_addr()
            .expect("the bound address")
            .to_string();

        let served = std::thread::spawn(move || {
            let (mut socket, _peer) = listener.accept().expect("one connection");
            let mut request = [0_u8; 512];
            let read = socket.read(&mut request).expect("the request");

            socket.write_all(response).expect("the response");
            // The reader is delimited by the close, so this is the response's
            // last byte as far as `fetch` is concerned.
            drop(socket);

            String::from_utf8_lossy(&request[..read]).into_owned()
        });

        (authority, served)
    }

    #[test]
    fn gets_the_url_and_returns_the_body() {
        let (authority, served) =
            one_shot_server(b"HTTP/1.1 200 OK\r\nContent-Type: image/png\r\n\r\nPNGBYTES");

        let fetched =
            futures::executor::block_on(fetch(&format!("http://{authority}/attachments/att-1")));

        assert_eq!(fetched.unwrap(), b"PNGBYTES");

        let request = served.join().expect("the server thread");

        assert!(
            request.starts_with("GET /attachments/att-1 HTTP/1.1\r\n"),
            "unexpected request line in {request:?}"
        );
        assert!(request.contains(&format!("Host: {authority}\r\n")));
        assert!(
            request.contains("Connection: close\r\n"),
            "the close is what delimits the body"
        );
    }

    #[test]
    fn reports_a_status_that_is_not_a_200() {
        let (authority, served) = one_shot_server(b"HTTP/1.1 404 Not Found\r\n\r\nno such file");

        let fetched =
            futures::executor::block_on(fetch(&format!("http://{authority}/attachments/gone")));

        assert!(fetched.unwrap_err().contains("404"));

        served.join().expect("the server thread");
    }
}
