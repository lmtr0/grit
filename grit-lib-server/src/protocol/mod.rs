//! Git wire protocol helpers for server-backed repositories.

pub mod canonical_clone;
pub mod canonical_clone_serve;
pub mod clone_admission;
pub mod clone_metrics;
pub mod delta_pack;
#[cfg(feature = "externalized-postgres")]
pub mod external_pack_promotion;
pub mod memory_pack_promotion;
pub mod negotiation;
pub mod pack_entry_reuse;
pub mod pack_stream;
pub mod pack_writer;
#[cfg(feature = "sqlx-postgres")]
pub mod pg_pack_promotion;
pub mod push_atomic_publication;
pub mod push_fix_thin;
pub mod push_metrics;
pub mod push_pack_validation;
pub mod push_prepared;
pub mod push_quarantine;
pub mod receive_pack;
pub mod receive_pack_stream;
pub mod upload_pack;
pub mod upload_pack_wire;
