use async_trait::async_trait;
use grit_lib::objects::ObjectId;
use sqlx::Row;
use time::OffsetDateTime;

use super::{
    PushExternalDeleteAuthorization, PushExternalOrphanCandidate, PushExternalOrphanMetadata,
    PushExternalOrphanObject, PushExternalOrphanOutcome, PushOrphanBackendError, PushOrphanLease,
    PushOrphanSweepLimits, PushOrphanWorkerToken,
};
use crate::ids::{RepositoryId, TenantId};
use crate::protocol::push_quarantine::QuarantineId;
use crate::sqlx_postgres::{lock_external_storage_key, lock_import_repository, PgServerStorage};

struct LockedCandidate {
    repository_pk: i64,
    tenant: TenantId,
    repository: RepositoryId,
    backend: String,
    storage_key: String,
    quarantine_id: QuarantineId,
    quarantine_generation: u64,
    prepared_fingerprint: ObjectId,
    pack_checksum: ObjectId,
    size_bytes: u64,
    storage_version: Option<Vec<u8>>,
    storage_checksum: Option<ObjectId>,
    not_before: OffsetDateTime,
    lease_generation: u64,
}

#[async_trait]
impl PushExternalOrphanMetadata for PgServerStorage {
    async fn lease_external_candidates(
        &self,
        worker: PushOrphanWorkerToken,
        observed_at: OffsetDateTime,
        limits: &PushOrphanSweepLimits,
    ) -> Result<Vec<PushExternalOrphanCandidate>, PushOrphanBackendError> {
        let fetch_limit =
            i64::try_from(limits.max_candidates).map_err(|_| PushOrphanBackendError)?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| PushOrphanBackendError)?;
        let rows = sqlx::query(
            "select orphan.repository_pk, repository.tenant_id, repository.repository_id,
                    orphan.storage_backend, orphan.storage_key, orphan.quarantine_id,
                    orphan.quarantine_generation, orphan.prepared_fingerprint,
                    orphan.pack_checksum, orphan.size_bytes, orphan.storage_version,
                    orphan.storage_checksum, orphan.not_before, orphan.lease_generation
             from grit_external_pack_promotion_orphans orphan
             join grit_repositories repository
               on repository.repository_pk = orphan.repository_pk
             where repository.deleted_at is null
               and orphan.not_before <= $1
               and (orphan.lease_expires_at is null or orphan.lease_expires_at <= $1)
               and (orphan.last_observed_at is null or orphan.last_observed_at <= $1)
             order by orphan.not_before, orphan.repository_pk, orphan.storage_backend,
                      orphan.storage_key, orphan.quarantine_id, orphan.quarantine_generation
             limit $2
             for update of orphan skip locked",
        )
        .bind(observed_at)
        .bind(fetch_limit)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| PushOrphanBackendError)?;

        let mut locked = Vec::new();
        locked
            .try_reserve_exact(rows.len())
            .map_err(|_| PushOrphanBackendError)?;
        for row in rows {
            locked.push(decode_locked_candidate(&row)?);
        }

        let mut selected = Vec::new();
        selected
            .try_reserve_exact(locked.len())
            .map_err(|_| PushOrphanBackendError)?;
        let mut selected_bytes = 0_u64;
        let mut selected_key_bytes = 0_usize;
        for candidate in locked {
            if candidate.size_bytes > limits.hard_candidate_bytes {
                continue;
            }
            let next_keys = selected_key_bytes
                .checked_add(candidate.storage_key.len())
                .ok_or(PushOrphanBackendError)?;
            if next_keys > limits.max_key_bytes {
                continue;
            }
            let oversized = candidate.size_bytes > limits.max_batch_bytes;
            if oversized && !selected.is_empty() {
                continue;
            }
            let next_bytes = selected_bytes
                .checked_add(candidate.size_bytes)
                .ok_or(PushOrphanBackendError)?;
            if !oversized && next_bytes > limits.max_batch_bytes {
                continue;
            }
            let next_generation = candidate
                .lease_generation
                .checked_add(1)
                .ok_or(PushOrphanBackendError)?;
            let updated = sqlx::query(
                "update grit_external_pack_promotion_orphans
                 set lease_token = $1, lease_expires_at = $2, lease_generation = $3,
                     last_observed_at = greatest(coalesce(last_observed_at, $4), $4)
                 where repository_pk = $5 and storage_backend = $6 and storage_key = $7
                   and quarantine_id = $8 and quarantine_generation = $9
                   and lease_generation = $10
                   and (lease_expires_at is null or lease_expires_at <= $4)
                   and (last_observed_at is null or last_observed_at <= $4)",
            )
            .bind(worker.as_bytes().as_slice())
            .bind(limits.lease_expires_at)
            .bind(i64::try_from(next_generation).map_err(|_| PushOrphanBackendError)?)
            .bind(observed_at)
            .bind(candidate.repository_pk)
            .bind(&candidate.backend)
            .bind(&candidate.storage_key)
            .bind(candidate.quarantine_id.as_bytes().as_slice())
            .bind(
                i64::try_from(candidate.quarantine_generation)
                    .map_err(|_| PushOrphanBackendError)?,
            )
            .bind(i64::try_from(candidate.lease_generation).map_err(|_| PushOrphanBackendError)?)
            .execute(&mut *transaction)
            .await
            .map_err(|_| PushOrphanBackendError)?;
            if updated.rows_affected() != 1 {
                return Err(PushOrphanBackendError);
            }
            selected_bytes = next_bytes;
            selected_key_bytes = next_keys;
            let object = match candidate.storage_version {
                Some(version) => Some(PushExternalOrphanObject {
                    size_bytes: candidate.size_bytes,
                    version,
                    checksum: candidate.storage_checksum.ok_or(PushOrphanBackendError)?,
                }),
                None => None,
            };
            selected.push(PushExternalOrphanCandidate {
                repository_pk: candidate.repository_pk,
                tenant: candidate.tenant,
                repository: candidate.repository,
                backend: candidate.backend,
                storage_key: candidate.storage_key,
                quarantine_id: candidate.quarantine_id,
                quarantine_generation: candidate.quarantine_generation,
                prepared_fingerprint: candidate.prepared_fingerprint,
                pack_checksum: candidate.pack_checksum,
                size_bytes: candidate.size_bytes,
                object,
                not_before: candidate.not_before,
                lease: PushOrphanLease {
                    worker,
                    generation: next_generation,
                    expires_at: limits.lease_expires_at,
                },
            });
            if oversized {
                break;
            }
        }
        transaction
            .commit()
            .await
            .map_err(|_| PushOrphanBackendError)?;
        Ok(selected)
    }

    async fn authorize_external_delete(
        &self,
        candidate: &PushExternalOrphanCandidate,
        observed_at: OffsetDateTime,
    ) -> Result<PushExternalDeleteAuthorization, PushOrphanBackendError> {
        if candidate.object.is_none() {
            return Ok(PushExternalDeleteAuthorization::Retry);
        }
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| PushOrphanBackendError)?;
        lock_import_repository(&mut transaction, &candidate.tenant, &candidate.repository)
            .await
            .map_err(|_| PushOrphanBackendError)?;
        lock_external_storage_key(&mut transaction, &candidate.backend, &candidate.storage_key)
            .await
            .map_err(|_| PushOrphanBackendError)?;
        let exists: Option<i32> = sqlx::query_scalar(
            "select 1 from grit_external_pack_promotion_orphans
                where repository_pk = $1 and storage_backend = $2 and storage_key = $3
                  and quarantine_id = $4 and quarantine_generation = $5
                  and prepared_fingerprint = $6 and pack_checksum = $7 and size_bytes = $8
                  and storage_version = $9 and storage_checksum = $10
                  and lease_token = $11 and lease_generation = $12
                  and lease_expires_at > $13 and not_before <= $13
                  and (last_observed_at is null or last_observed_at <= $13)
             for update",
        )
        .bind(candidate.repository_pk)
        .bind(&candidate.backend)
        .bind(&candidate.storage_key)
        .bind(candidate.quarantine_id.as_bytes().as_slice())
        .bind(i64::try_from(candidate.quarantine_generation).map_err(|_| PushOrphanBackendError)?)
        .bind(candidate.prepared_fingerprint.as_bytes())
        .bind(candidate.pack_checksum.as_bytes())
        .bind(i64::try_from(candidate.size_bytes).map_err(|_| PushOrphanBackendError)?)
        .bind(
            candidate
                .object
                .as_ref()
                .map(|object| object.version.as_slice()),
        )
        .bind(
            candidate
                .object
                .as_ref()
                .map(|object| object.checksum.as_bytes()),
        )
        .bind(candidate.lease.worker.as_bytes().as_slice())
        .bind(i64::try_from(candidate.lease.generation).map_err(|_| PushOrphanBackendError)?)
        .bind(observed_at)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| PushOrphanBackendError)?;
        if exists.is_none() {
            transaction
                .rollback()
                .await
                .map_err(|_| PushOrphanBackendError)?;
            return Ok(PushExternalDeleteAuthorization::Retry);
        }

        let protected: bool = sqlx::query_scalar(
            "select exists (
                 select 1 from grit_packs
                  where storage_backend = $1 and storage_key = $2
                 union all
                 select 1 from grit_push_pack_receipts
                  where storage_backend = $1 and storage_key = $2
                 union all
                 select 1 from grit_external_pack_promotion_orphans other
                  where other.storage_backend = $1 and other.storage_key = $2
                    and not (other.repository_pk = $3 and other.quarantine_id = $4
                        and other.quarantine_generation = $5)
                    and (other.not_before > $6 or other.lease_expires_at > $6
                        or other.delete_state = 'deleting')
             )",
        )
        .bind(&candidate.backend)
        .bind(&candidate.storage_key)
        .bind(candidate.repository_pk)
        .bind(candidate.quarantine_id.as_bytes().as_slice())
        .bind(i64::try_from(candidate.quarantine_generation).map_err(|_| PushOrphanBackendError)?)
        .bind(observed_at)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| PushOrphanBackendError)?;
        if protected {
            transaction
                .commit()
                .await
                .map_err(|_| PushOrphanBackendError)?;
            Ok(PushExternalDeleteAuthorization::Protected)
        } else {
            let updated = sqlx::query(
                "update grit_external_pack_promotion_orphans
                 set delete_state = 'deleting',
                     last_observed_at = greatest(coalesce(last_observed_at, $1), $1)
                 where repository_pk = $2 and storage_backend = $3 and storage_key = $4
                   and quarantine_id = $5 and quarantine_generation = $6
                   and prepared_fingerprint = $7 and pack_checksum = $8 and size_bytes = $9
                   and storage_version = $10 and storage_checksum = $11
                   and lease_token = $12 and lease_generation = $13
                   and lease_expires_at > $1",
            )
            .bind(observed_at)
            .bind(candidate.repository_pk)
            .bind(&candidate.backend)
            .bind(&candidate.storage_key)
            .bind(candidate.quarantine_id.as_bytes().as_slice())
            .bind(
                i64::try_from(candidate.quarantine_generation)
                    .map_err(|_| PushOrphanBackendError)?,
            )
            .bind(candidate.prepared_fingerprint.as_bytes())
            .bind(candidate.pack_checksum.as_bytes())
            .bind(i64::try_from(candidate.size_bytes).map_err(|_| PushOrphanBackendError)?)
            .bind(
                candidate
                    .object
                    .as_ref()
                    .map(|object| object.version.as_slice()),
            )
            .bind(
                candidate
                    .object
                    .as_ref()
                    .map(|object| object.checksum.as_bytes()),
            )
            .bind(candidate.lease.worker.as_bytes().as_slice())
            .bind(i64::try_from(candidate.lease.generation).map_err(|_| PushOrphanBackendError)?)
            .execute(&mut *transaction)
            .await
            .map_err(|_| PushOrphanBackendError)?;
            if updated.rows_affected() != 1 {
                return Err(PushOrphanBackendError);
            }
            transaction
                .commit()
                .await
                .map_err(|_| PushOrphanBackendError)?;
            Ok(PushExternalDeleteAuthorization::DeleteExact)
        }
    }

    async fn acknowledge_external_outcome(
        &self,
        candidate: &PushExternalOrphanCandidate,
        outcome: PushExternalOrphanOutcome,
        observed_at: OffsetDateTime,
        retry_not_before: OffsetDateTime,
    ) -> Result<(), PushOrphanBackendError> {
        let result = if outcome == PushExternalOrphanOutcome::Retry {
            sqlx::query(
                "update grit_external_pack_promotion_orphans
                 set lease_token = null, lease_expires_at = null,
                     not_before = greatest(not_before, $1), last_outcome = 'retry',
                     last_observed_at = greatest(coalesce(last_observed_at, $2), $2),
                     delete_state = 'idle'
                 where repository_pk = $3 and storage_backend = $4 and storage_key = $5
                   and quarantine_id = $6 and quarantine_generation = $7
                   and prepared_fingerprint = $8 and pack_checksum = $9 and size_bytes = $10
                   and storage_version is not distinct from $11
                   and storage_checksum is not distinct from $12
                   and lease_token = $13 and lease_generation = $14
                   and delete_state in ('idle', 'deleting')",
            )
            .bind(retry_not_before)
            .bind(observed_at)
            .bind(candidate.repository_pk)
            .bind(&candidate.backend)
            .bind(&candidate.storage_key)
            .bind(candidate.quarantine_id.as_bytes().as_slice())
            .bind(
                i64::try_from(candidate.quarantine_generation)
                    .map_err(|_| PushOrphanBackendError)?,
            )
            .bind(candidate.prepared_fingerprint.as_bytes())
            .bind(candidate.pack_checksum.as_bytes())
            .bind(i64::try_from(candidate.size_bytes).map_err(|_| PushOrphanBackendError)?)
            .bind(
                candidate
                    .object
                    .as_ref()
                    .map(|object| object.version.as_slice()),
            )
            .bind(
                candidate
                    .object
                    .as_ref()
                    .map(|object| object.checksum.as_bytes()),
            )
            .bind(candidate.lease.worker.as_bytes().as_slice())
            .bind(i64::try_from(candidate.lease.generation).map_err(|_| PushOrphanBackendError)?)
            .execute(self.pool())
            .await
            .map_err(|_| PushOrphanBackendError)?
        } else {
            sqlx::query(
                "delete from grit_external_pack_promotion_orphans
                 where repository_pk = $1 and storage_backend = $2 and storage_key = $3
                   and quarantine_id = $4 and quarantine_generation = $5
                   and prepared_fingerprint = $6 and pack_checksum = $7 and size_bytes = $8
                   and storage_version is not distinct from $9
                   and storage_checksum is not distinct from $10
                   and lease_token = $11 and lease_generation = $12
                   and ($13 or delete_state = 'deleting')",
            )
            .bind(candidate.repository_pk)
            .bind(&candidate.backend)
            .bind(&candidate.storage_key)
            .bind(candidate.quarantine_id.as_bytes().as_slice())
            .bind(
                i64::try_from(candidate.quarantine_generation)
                    .map_err(|_| PushOrphanBackendError)?,
            )
            .bind(candidate.prepared_fingerprint.as_bytes())
            .bind(candidate.pack_checksum.as_bytes())
            .bind(i64::try_from(candidate.size_bytes).map_err(|_| PushOrphanBackendError)?)
            .bind(
                candidate
                    .object
                    .as_ref()
                    .map(|object| object.version.as_slice()),
            )
            .bind(
                candidate
                    .object
                    .as_ref()
                    .map(|object| object.checksum.as_bytes()),
            )
            .bind(candidate.lease.worker.as_bytes().as_slice())
            .bind(i64::try_from(candidate.lease.generation).map_err(|_| PushOrphanBackendError)?)
            .bind(outcome == PushExternalOrphanOutcome::Protected)
            .execute(self.pool())
            .await
            .map_err(|_| PushOrphanBackendError)?
        };
        if result.rows_affected() != 1 {
            return Err(PushOrphanBackendError);
        }
        Ok(())
    }
}

fn decode_locked_candidate(
    row: &sqlx::postgres::PgRow,
) -> Result<LockedCandidate, PushOrphanBackendError> {
    let quarantine_id = fixed_token(
        row.try_get("quarantine_id")
            .map_err(|_| PushOrphanBackendError)?,
    )?;
    let storage_version: Option<Vec<u8>> = row
        .try_get("storage_version")
        .map_err(|_| PushOrphanBackendError)?;
    let storage_checksum = row
        .try_get::<Option<Vec<u8>>, _>("storage_checksum")
        .map_err(|_| PushOrphanBackendError)?
        .map(|bytes| ObjectId::from_bytes(&bytes).map_err(|_| PushOrphanBackendError))
        .transpose()?;
    if storage_version.is_some() != storage_checksum.is_some() {
        return Err(PushOrphanBackendError);
    }
    Ok(LockedCandidate {
        repository_pk: row
            .try_get("repository_pk")
            .map_err(|_| PushOrphanBackendError)?,
        tenant: TenantId::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(|_| PushOrphanBackendError)?,
        )
        .map_err(|_| PushOrphanBackendError)?,
        repository: RepositoryId::new(
            row.try_get::<String, _>("repository_id")
                .map_err(|_| PushOrphanBackendError)?,
        )
        .map_err(|_| PushOrphanBackendError)?,
        backend: row
            .try_get("storage_backend")
            .map_err(|_| PushOrphanBackendError)?,
        storage_key: row
            .try_get("storage_key")
            .map_err(|_| PushOrphanBackendError)?,
        quarantine_id: QuarantineId::from_random_bytes(quarantine_id)
            .map_err(|_| PushOrphanBackendError)?,
        quarantine_generation: positive_u64(
            row.try_get("quarantine_generation")
                .map_err(|_| PushOrphanBackendError)?,
        )?,
        prepared_fingerprint: object_id(
            row.try_get("prepared_fingerprint")
                .map_err(|_| PushOrphanBackendError)?,
        )?,
        pack_checksum: object_id(
            row.try_get("pack_checksum")
                .map_err(|_| PushOrphanBackendError)?,
        )?,
        size_bytes: nonnegative_u64(
            row.try_get("size_bytes")
                .map_err(|_| PushOrphanBackendError)?,
        )?,
        storage_version,
        storage_checksum,
        not_before: row
            .try_get("not_before")
            .map_err(|_| PushOrphanBackendError)?,
        lease_generation: nonnegative_u64(
            row.try_get("lease_generation")
                .map_err(|_| PushOrphanBackendError)?,
        )?,
    })
}

fn fixed_token(bytes: Vec<u8>) -> Result<[u8; 32], PushOrphanBackendError> {
    bytes.try_into().map_err(|_| PushOrphanBackendError)
}

fn object_id(bytes: Vec<u8>) -> Result<ObjectId, PushOrphanBackendError> {
    ObjectId::from_bytes(&bytes).map_err(|_| PushOrphanBackendError)
}

fn positive_u64(value: i64) -> Result<u64, PushOrphanBackendError> {
    let value = u64::try_from(value).map_err(|_| PushOrphanBackendError)?;
    if value == 0 {
        return Err(PushOrphanBackendError);
    }
    Ok(value)
}

fn nonnegative_u64(value: i64) -> Result<u64, PushOrphanBackendError> {
    u64::try_from(value).map_err(|_| PushOrphanBackendError)
}
