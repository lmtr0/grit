//! NATS invalidation publisher and local-cache subscriber.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;

use crate::cache::{apply_invalidation, Cache, EventPublisher, InvalidationEvent};
use crate::error::{Error, Result};

/// NATS publisher for cache invalidation events.
#[derive(Clone)]
pub struct NatsInvalidationPublisher {
    client: async_nats::Client,
    subject: String,
}

impl NatsInvalidationPublisher {
    /// Create a publisher from an existing NATS client and subject.
    #[must_use]
    pub fn new(client: async_nats::Client, subject: impl Into<String>) -> Self {
        Self {
            client,
            subject: subject.into(),
        }
    }

    /// Connect to NATS and create an invalidation publisher.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Cache`] when NATS cannot be reached.
    pub async fn connect(server: &str, subject: impl Into<String>) -> Result<Self> {
        let client = async_nats::connect(server)
            .await
            .map_err(|err| Error::Cache(format!("connect nats invalidation publisher: {err}")))?;
        Ok(Self::new(client, subject))
    }

    /// Borrow the configured NATS subject.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

#[async_trait]
impl EventPublisher for NatsInvalidationPublisher {
    async fn publish_invalidation(&self, event: InvalidationEvent) -> Result<()> {
        self.client
            .publish(self.subject.clone(), event.to_json_bytes()?.into())
            .await
            .map_err(|err| Error::Cache(format!("publish nats invalidation: {err}")))
    }
}

/// NATS subscriber that applies invalidation events to a local cache.
pub struct NatsInvalidationSubscriber<C> {
    client: async_nats::Client,
    subject: String,
    cache: Arc<C>,
}

impl<C> NatsInvalidationSubscriber<C>
where
    C: Cache,
{
    /// Create a subscriber from an existing NATS client, subject, and cache.
    #[must_use]
    pub fn new(client: async_nats::Client, subject: impl Into<String>, cache: Arc<C>) -> Self {
        Self {
            client,
            subject: subject.into(),
            cache,
        }
    }

    /// Connect to NATS and create a subscriber that invalidates `cache`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Cache`] when NATS cannot be reached.
    pub async fn connect(server: &str, subject: impl Into<String>, cache: Arc<C>) -> Result<Self> {
        let client = async_nats::connect(server)
            .await
            .map_err(|err| Error::Cache(format!("connect nats invalidation subscriber: {err}")))?;
        Ok(Self::new(client, subject, cache))
    }

    /// Subscribe and process invalidation events until the NATS subscription ends.
    ///
    /// # Errors
    ///
    /// Returns NATS subscription errors, event decode errors, or cache invalidation errors.
    pub async fn run_until_closed(self) -> Result<()> {
        let mut subscriber = self
            .client
            .subscribe(self.subject)
            .await
            .map_err(|err| Error::Cache(format!("subscribe nats invalidation: {err}")))?;
        while let Some(message) = subscriber.next().await {
            let event = InvalidationEvent::from_json_slice(&message.payload)?;
            apply_invalidation(self.cache.as_ref(), &event).await?;
        }
        Ok(())
    }
}
