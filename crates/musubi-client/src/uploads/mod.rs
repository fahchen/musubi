//! Uploads, in three layers (BDR-0024–BDR-0028, `docs/uploads.md`,
//! `docs/rust-client.md` §10).
//!
//! ```text
//! transfer   the control plane: preflight, chunk transfer, external uploaders
//!    │                          — the only writer of UploadHandle::status
//!    ▼
//! registry   the data plane: folds upload_ops into per-(store_id, name) cells
//!    │                       and hands out the Upload both halves live on
//!    ▼
//! ops        the wire vocabulary: UploadOp and everything it carries
//! ```
//!
//! Every arrow points down. `ops` knows nothing about the rest — which is what
//! lets the envelope decode `upload_ops` without pulling the transfer machinery
//! in — and only the control plane reaches into a cell. The registry does name two control-plane types, `EntryTransport` and
//! `UploadControl`, but never looks inside either: it holds the transport state
//! a cell owns and the connection seam a handed-out handle carries.
//!
//! Everything public is re-exported here and again from the crate root, which
//! is the only path an embedder ever writes.

mod ops;
mod registry;
mod transfer;

pub use self::ops::{
    EntryStatus, UploadAccept, UploadConfig, UploadEntry, UploadError, UploadErrorCode,
    UploadStatus,
};
pub use self::registry::{Upload, UploadHandle};

// The wire vocabulary and the registry are crate-internal: folding an envelope
// by hand is not a supported entry point (`docs/rust-reactive-state.md` §5.5).
pub(crate) use self::ops::UploadOp;
pub(crate) use self::registry::Uploads;
pub use self::transfer::{
    CancelSignal, UploadFile, UploadProgress, UploadRequest, Uploader, UploaderError,
};

// The connection seam: built by the mount call, held by the root cell that
// hands out `Upload`s (§10.2).
pub(crate) use self::transfer::UploadControl;
