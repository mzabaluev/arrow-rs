use std::ops::Range;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt};

use crate::errors::AvroError;
use crate::reader::async_reader::AsyncFileReader;

pub struct ObjectStoreReader<S> {
    store: S,
    path: Path,
}

impl<S> ObjectStoreReader<S> {
    pub fn new(store: S, path: Path) -> Self {
        Self { store, path }
    }
}

impl<S: ObjectStore> AsyncFileReader for ObjectStoreReader<S> {
    fn get_stream(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, Result<BoxStream<'_, Result<Bytes, AvroError>>, AvroError>> {
        async {
            let options = GetOptions::new().with_range(Some(range));
            let get_result = self
                .store
                .get_opts(&self.path, options)
                .await
                .map_err(|e| AvroError::External(Box::new(e)))?;
            let stream = get_result
                .into_stream()
                .map_err(|e| AvroError::External(Box::new(e)))
                .boxed();
            Ok(stream)
        }
        .boxed()
    }

    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, AvroError>> {
        async {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(|e| AvroError::External(Box::new(e)))
        }
        .boxed()
    }
}
