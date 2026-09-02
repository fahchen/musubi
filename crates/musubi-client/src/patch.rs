//! RFC 6902 application (`docs/rust-client.md` §4.1).
//!
//! Pointer unescaping, array-index rules and sequential left-to-right
//! application are the `json-patch` crate's contract; this module adds the two
//! things the Musubi layer owns — the op allowlist (enforced one step earlier,
//! at [`PatchEnvelope::decode`](crate::PatchEnvelope::decode)) and the mapping
//! of the crate's failures onto [`PatchError`].

use json_patch::PatchOperation;
use json_patch::jsonptr::PointerBuf;
use json_patch::{AddOperation, RemoveOperation, ReplaceOperation};
use serde_json::Value;

use crate::envelope::PatchOp;
use crate::error::PatchError;

/// Applies `ops` to `doc` left to right, atomically.
///
/// On any failure `doc` is left exactly as it was: pointers are parsed up
/// front, before a single op runs, and `json_patch::patch` unwinds its own
/// partial application.
pub(crate) fn apply_ops(doc: &mut Value, ops: &[PatchOp]) -> Result<(), PatchError> {
    let operations = ops
        .iter()
        .map(to_operation)
        .collect::<Result<Vec<_>, PatchError>>()?;

    json_patch::patch(doc, &operations).map_err(PatchError::from)
}

/// Rebuilds the `json-patch` op, parsing the pointer.
fn to_operation(op: &PatchOp) -> Result<PatchOperation, PatchError> {
    Ok(match op {
        PatchOp::Add { path, value } => PatchOperation::Add(AddOperation {
            path: parse_pointer(path)?,
            value: value.clone(),
        }),
        PatchOp::Remove { path } => PatchOperation::Remove(RemoveOperation {
            path: parse_pointer(path)?,
        }),
        PatchOp::Replace { path, value } => PatchOperation::Replace(ReplaceOperation {
            path: parse_pointer(path)?,
            value: value.clone(),
        }),
    })
}

/// Parses an RFC 6901 pointer, mapping a syntax error onto [`PatchError`].
fn parse_pointer(path: &str) -> Result<PointerBuf, PatchError> {
    PointerBuf::parse(path).map_err(|_| PatchError::InvalidPointer {
        path: path.to_owned(),
    })
}
