//! Git wire protocol helpers for server-backed repositories.

pub mod canonical_clone;
pub mod canonical_clone_serve;
pub mod clone_metrics;
pub mod delta_pack;
pub mod negotiation;
pub mod pack_entry_reuse;
pub mod pack_stream;
pub mod pack_writer;
pub mod receive_pack;
pub mod upload_pack;
pub mod upload_pack_wire;
