//! Converting values to and from [`Payload`]s.
//!
//! A payload carries every input and result that crosses the wire: workflow and task
//! arguments, task results, and handler inputs and outputs. It holds the encoded bytes plus
//! string metadata such as `encoding` and `content-type`.
//!
//! JSON is the default encoding, and [`PayloadCodec`] applies it to every serde type. The
//! module also offers bincode and, with the `compression` feature, gzip-compressed JSON.
//! Decoders read only the payload bytes and ignore its metadata, so decode a payload with
//! the function that matches how it was encoded.
//!
//! ## Quick Start
//!
//! ```rust
//! use orcher::prelude::*;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct OrderInput {
//!     order_id: String,
//!     amount: f64,
//! }
//!
//! // Serialize to payload
//! let input = OrderInput {
//!     order_id: "order-123".to_string(),
//!     amount: 99.99,
//! };
//!
//! let payload = to_payload(&input).unwrap();
//!
//! // Deserialize from payload
//! let decoded: OrderInput = from_payload(&payload).unwrap();
//! ```
//!
//! ## Custom Serialization
//!
//! Types that do not implement serde's traits can implement [`PayloadCodec`] themselves:
//!
//! ```rust
//! use orcher::payload::PayloadCodec;
//! use orcher::Result;
//!
//! struct CustomType {
//!     data: Vec<u8>,
//! }
//!
//! impl PayloadCodec for CustomType {
//!     fn encode(&self) -> Result<Vec<u8>> {
//!         Ok(self.data.clone())
//!     }
//!
//!     fn decode(bytes: &[u8]) -> Result<Self> {
//!         Ok(Self { data: bytes.to_vec() })
//!     }
//! }
//!
//! let payload = CustomType { data: vec![1, 2, 3] }.to_payload()?;
//! assert_eq!(CustomType::from_payload(&payload)?.data, vec![1, 2, 3]);
//! # Ok::<(), orcher::Error>(())
//! ```

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub use orcher_sdk_core::payload::Payload;

/// A type that can be encoded to and decoded from a [`Payload`].
///
/// Every type that implements serde's `Serialize` and `Deserialize` gets this trait
/// through a blanket implementation that uses JSON and sets the JSON payload metadata.
/// Implement it by hand only for types without serde support; such an implementation
/// supplies `encode` and `decode`, and the provided methods wrap the bytes in a payload
/// without metadata.
///
/// ## Example
///
/// ```rust
/// use orcher::prelude::*;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct MyData {
///     value: i32,
/// }
///
/// // `MyData` implements `PayloadCodec` through the blanket implementation.
/// ```
pub trait PayloadCodec: Sized {
    /// Encodes this value to bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be encoded.
    fn encode(&self) -> Result<Vec<u8>>;

    /// Decodes a value from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid encoding of `Self`.
    fn decode(bytes: &[u8]) -> Result<Self>;

    /// Encodes this value into a payload.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be encoded.
    fn to_payload(&self) -> Result<Payload> {
        let bytes = self.encode()?;
        Ok(Payload::new_data(bytes))
    }

    /// Decodes a value from a payload's bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid encoding of `Self`.
    fn from_payload(payload: &Payload) -> Result<Self> {
        Self::decode(&payload.data)
    }
}

/// Method-call syntax for decoding a [`Payload`].
///
/// ## Example
///
/// ```rust
/// use orcher::prelude::*;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct MyData { value: i32 }
///
/// let payload = to_payload(&MyData { value: 7 })?;
///
/// // Method syntax:
/// let data: MyData = payload.from_payload()?;
///
/// // Equivalent associated-function syntax:
/// let data = MyData::from_payload(&payload)?;
/// assert_eq!(data.value, 7);
/// # Ok::<(), orcher::Error>(())
/// ```
pub trait PayloadExt {
    /// Decodes this payload as a `T`; the same as `T::from_payload(self)`.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload bytes are not a valid encoding of `T`.
    // Named after `PayloadCodec::from_payload`, which it forwards to; renaming it to
    // satisfy the `from_*` convention would break every caller.
    #[allow(clippy::wrong_self_convention)]
    fn from_payload<T: PayloadCodec>(&self) -> Result<T>;
}

impl PayloadExt for Payload {
    fn from_payload<T: PayloadCodec>(&self) -> Result<T> {
        T::from_payload(self)
    }
}

// JSON for every serde type. The payload methods are overridden so payloads carry the
// JSON `encoding` and `content-type` metadata.
impl<T> PayloadCodec for T
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| Error::Serialization(e.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))
    }

    fn to_payload(&self) -> Result<Payload> {
        Payload::from_json(self).map_err(|e| Error::Serialization(e.to_string()))
    }

    fn from_payload(payload: &Payload) -> Result<Self> {
        payload
            .to_json()
            .map_err(|e| Error::Serialization(e.to_string()))
    }
}

/// Encodes a value as a JSON [`Payload`].
///
/// This is the usual way to build workflow and task inputs.
///
/// # Errors
///
/// Returns [`Error::Serialization`] if the value cannot be serialized.
///
/// # Example
///
/// ```rust
/// use orcher::prelude::*;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Input {
///     name: String,
/// }
///
/// let input = Input { name: "Alice".to_string() };
/// let payload = to_payload(&input).unwrap();
/// ```
pub fn to_payload<T: Serialize>(value: &T) -> Result<Payload> {
    Payload::from_json(value).map_err(|e| Error::Serialization(e.to_string()))
}

/// Decodes a value from a [`Payload`] using the type's [`PayloadCodec`].
///
/// For serde types this reads the payload as JSON.
///
/// # Errors
///
/// Returns an error if the payload bytes are not a valid encoding of `T`.
///
/// # Example
///
/// ```rust
/// use orcher::prelude::*;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Output {
///     result: i32,
/// }
///
/// # let payload = to_payload(&Output { result: 42 }).unwrap();
/// let output: Output = from_payload(&payload).unwrap();
/// assert_eq!(output.result, 42);
/// ```
pub fn from_payload<T: PayloadCodec>(payload: &Payload) -> Result<T> {
    T::from_payload(payload)
}

/// Serializes a value to JSON bytes.
///
/// # Errors
///
/// Returns [`Error::Serialization`] if the value cannot be serialized.
///
/// # Example
///
/// ```rust
/// use orcher::payload::to_json_bytes;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Data {
///     value: i32,
/// }
///
/// let data = Data { value: 42 };
/// let bytes = to_json_bytes(&data).unwrap();
/// ```
pub fn to_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| Error::Serialization(e.to_string()))
}

/// Deserializes a value from JSON bytes.
///
/// # Errors
///
/// Returns [`Error::Serialization`] if the bytes are not valid JSON for `T`.
///
/// # Example
///
/// ```rust
/// use orcher::payload::{from_json_bytes, to_json_bytes};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Data {
///     value: i32,
/// }
///
/// # let bytes = to_json_bytes(&Data { value: 42 }).unwrap();
/// let data: Data = from_json_bytes(&bytes).unwrap();
/// assert_eq!(data.value, 42);
/// ```
pub fn from_json_bytes<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))
}

/// Serializes a value to a pretty-printed JSON string, for logging and debugging.
///
/// # Errors
///
/// Returns [`Error::Serialization`] if the value cannot be serialized.
///
/// # Example
///
/// ```rust
/// use orcher::payload::to_json_string;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Data {
///     value: i32,
/// }
///
/// let data = Data { value: 42 };
/// let json = to_json_string(&data).unwrap();
/// println!("{}", json);
/// ```
pub fn to_json_string<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string_pretty(value).map_err(|e| Error::Serialization(e.to_string()))
}

/// Deserializes a value from a JSON string.
///
/// # Errors
///
/// Returns [`Error::Serialization`] if the string is not valid JSON for `T`.
///
/// # Example
///
/// ```rust
/// use orcher::payload::from_json_string;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Data {
///     value: i32,
/// }
///
/// let json = r#"{"value": 42}"#;
/// let data: Data = from_json_string(json).unwrap();
/// assert_eq!(data.value, 42);
/// ```
pub fn from_json_string<T: for<'de> Deserialize<'de>>(json: &str) -> Result<T> {
    serde_json::from_str(json).map_err(|e| Error::Serialization(e.to_string()))
}

/// Serializes a value to bincode bytes.
///
/// Bincode is more compact and faster than JSON, but not human-readable, and the bytes do
/// not describe their own layout: the reader must decode into the same type definition.
///
/// # Errors
///
/// Returns [`Error::Serialization`] if the value cannot be serialized.
///
/// # Example
///
/// ```rust
/// use orcher::payload::to_binary_bytes;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Data {
///     value: i32,
/// }
///
/// let data = Data { value: 42 };
/// let bytes = to_binary_bytes(&data).unwrap();
/// ```
pub fn to_binary_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::serialize(value).map_err(|e| Error::Serialization(e.to_string()))
}

/// Deserializes a value from bincode bytes.
///
/// # Errors
///
/// Returns [`Error::Serialization`] if the bytes are not a valid bincode encoding of `T`.
///
/// # Example
///
/// ```rust
/// use orcher::payload::{from_binary_bytes, to_binary_bytes};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Data {
///     value: i32,
/// }
///
/// # let bytes = to_binary_bytes(&Data { value: 42 }).unwrap();
/// let data: Data = from_binary_bytes(&bytes).unwrap();
/// assert_eq!(data.value, 42);
/// ```
pub fn from_binary_bytes<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    bincode::deserialize(bytes).map_err(|e| Error::Serialization(e.to_string()))
}

/// Encodes a value as a bincode [`Payload`].
///
/// Sets the `encoding` metadata to `bincode` and `content-type` to
/// `application/octet-stream`. Decode it with [`from_binary_payload`].
///
/// # Errors
///
/// Returns [`Error::Serialization`] if the value cannot be serialized.
///
/// # Example
///
/// ```rust
/// use orcher::payload::to_binary_payload;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Data {
///     value: i32,
/// }
///
/// let data = Data { value: 42 };
/// let payload = to_binary_payload(&data).unwrap();
/// ```
pub fn to_binary_payload<T: Serialize>(value: &T) -> Result<Payload> {
    let bytes = to_binary_bytes(value)?;
    let payload = Payload::new_data(bytes)
        .with_metadata("encoding", "bincode")
        .with_metadata("content-type", "application/octet-stream");
    Ok(payload)
}

/// Decodes a value from a payload made by [`to_binary_payload`].
///
/// # Errors
///
/// Returns [`Error::Serialization`] if the payload bytes are not a valid bincode encoding
/// of `T`.
///
/// # Example
///
/// ```rust
/// use orcher::payload::{from_binary_payload, to_binary_payload};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Data {
///     value: i32,
/// }
///
/// # let payload = to_binary_payload(&Data { value: 42 }).unwrap();
/// let data: Data = from_binary_payload(&payload).unwrap();
/// assert_eq!(data.value, 42);
/// ```
pub fn from_binary_payload<T: for<'de> Deserialize<'de>>(payload: &Payload) -> Result<T> {
    from_binary_bytes(&payload.data)
}

#[cfg(feature = "compression")]
use flate2::{read::GzDecoder, write::GzEncoder, Compression};

#[cfg(feature = "compression")]
use std::io::{Read, Write};

/// Compresses bytes with gzip.
///
/// Requires the `compression` feature.
///
/// # Errors
///
/// Returns [`Error::Serialization`] if compression fails.
///
/// # Example
///
/// ```rust
/// use orcher::payload::compress_bytes;
///
/// let data = b"Hello, World!".repeat(1000);
/// let compressed = compress_bytes(&data).unwrap();
/// assert!(compressed.len() < data.len());
/// ```
#[cfg(feature = "compression")]
pub fn compress_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(bytes)
        .map_err(|e| Error::Serialization(format!("Compression failed: {}", e)))?;
    encoder
        .finish()
        .map_err(|e| Error::Serialization(format!("Compression failed: {}", e)))
}

/// Decompresses gzip bytes.
///
/// Requires the `compression` feature.
///
/// # Errors
///
/// Returns [`Error::Serialization`] if the input is not valid gzip data.
///
/// # Example
///
/// ```rust
/// use orcher::payload::{compress_bytes, decompress_bytes};
///
/// let data = b"Hello, World!".repeat(1000);
/// let compressed = compress_bytes(&data).unwrap();
/// let decompressed = decompress_bytes(&compressed).unwrap();
/// assert_eq!(data, decompressed.as_slice());
/// ```
#[cfg(feature = "compression")]
pub fn decompress_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(bytes);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| Error::Serialization(format!("Decompression failed: {}", e)))?;
    Ok(decompressed)
}

/// Encodes a value as gzip-compressed JSON in a [`Payload`].
///
/// Sets the `encoding` metadata to `json+gzip`, `content-type` to `application/json`, and
/// `compressed` to `true`. Decode it with [`from_compressed_payload`]. Requires the
/// `compression` feature.
///
/// # Errors
///
/// Returns [`Error::Serialization`] if serialization or compression fails.
///
/// # Example
///
/// ```rust
/// use orcher::payload::to_compressed_payload;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct LargeData {
///     items: Vec<String>,
/// }
///
/// let data = LargeData { items: vec!["item".to_string(); 10000] };
/// let payload = to_compressed_payload(&data).unwrap();
/// ```
#[cfg(feature = "compression")]
pub fn to_compressed_payload<T: Serialize>(value: &T) -> Result<Payload> {
    let json_bytes = to_json_bytes(value)?;
    let compressed = compress_bytes(&json_bytes)?;
    let payload = Payload::new_data(compressed)
        .with_metadata("encoding", "json+gzip")
        .with_metadata("content-type", "application/json")
        .with_metadata("compressed", "true");
    Ok(payload)
}

/// Decodes a value from a payload made by [`to_compressed_payload`].
///
/// Requires the `compression` feature.
///
/// # Errors
///
/// Returns [`Error::Serialization`] if the payload bytes are not valid gzip data or do not
/// hold valid JSON for `T`.
///
/// # Example
///
/// ```rust
/// use orcher::payload::{to_compressed_payload, from_compressed_payload};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct LargeData {
///     items: Vec<String>,
/// }
///
/// # let data = LargeData { items: vec!["item".to_string(); 10000] };
/// # let payload = to_compressed_payload(&data).unwrap();
/// let decoded: LargeData = from_compressed_payload(&payload).unwrap();
/// ```
#[cfg(feature = "compression")]
pub fn from_compressed_payload<T: for<'de> Deserialize<'de>>(payload: &Payload) -> Result<T> {
    let decompressed = decompress_bytes(&payload.data)?;
    from_json_bytes(&decompressed)
}

/// Builds a [`Payload`] with custom metadata.
///
/// The `json`, `binary`, and `string` setters also set the matching `encoding` and
/// `content-type` metadata; `bytes` sets only the data. A later setter replaces the data
/// and overwrites those two keys.
///
/// # Example
///
/// ```rust
/// use orcher::payload::PayloadBuilder;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Data {
///     value: i32,
/// }
///
/// let data = Data { value: 42 };
/// let payload = PayloadBuilder::new()
///     .json(&data).unwrap()
///     .metadata("version", "1.0")
///     .metadata("source", "rust-sdk")
///     .build();
/// ```
pub struct PayloadBuilder {
    data: Vec<u8>,
    metadata: HashMap<String, Vec<u8>>,
}

impl PayloadBuilder {
    /// Creates a builder with empty data and no metadata.
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Sets the data to the JSON encoding of `value`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Serialization`] if the value cannot be serialized.
    pub fn json<T: Serialize>(mut self, value: &T) -> Result<Self> {
        self.data = to_json_bytes(value)?;
        self.metadata
            .insert("encoding".to_string(), b"json".to_vec());
        self.metadata
            .insert("content-type".to_string(), b"application/json".to_vec());
        Ok(self)
    }

    /// Sets the data to the bincode encoding of `value`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Serialization`] if the value cannot be serialized.
    pub fn binary<T: Serialize>(mut self, value: &T) -> Result<Self> {
        self.data = to_binary_bytes(value)?;
        self.metadata
            .insert("encoding".to_string(), b"bincode".to_vec());
        self.metadata.insert(
            "content-type".to_string(),
            b"application/octet-stream".to_vec(),
        );
        Ok(self)
    }

    /// Sets the data to raw bytes, leaving the metadata unchanged.
    pub fn bytes(mut self, bytes: Vec<u8>) -> Self {
        self.data = bytes;
        self
    }

    /// Sets the data to a UTF-8 string.
    pub fn string(mut self, value: impl Into<String>) -> Self {
        self.data = value.into().into_bytes();
        self.metadata
            .insert("encoding".to_string(), b"utf-8".to_vec());
        self.metadata
            .insert("content-type".to_string(), b"text/plain".to_vec());
        self
    }

    /// Adds a metadata entry, replacing any existing value for `key`.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Returns the finished payload.
    pub fn build(self) -> Payload {
        Payload {
            data: self.data,
            metadata: self.metadata,
        }
    }
}

impl Default for PayloadBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestData {
        name: String,
        value: i32,
    }

    #[test]
    fn test_to_from_payload() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let payload = to_payload(&data).unwrap();
        let decoded: TestData = from_payload(&payload).unwrap();

        assert_eq!(data, decoded);
    }

    #[test]
    fn test_payload_codec_trait() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let payload = data.to_payload().unwrap();
        let decoded = TestData::from_payload(&payload).unwrap();

        assert_eq!(data, decoded);
    }

    #[test]
    fn test_json_bytes() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let bytes = to_json_bytes(&data).unwrap();
        let decoded: TestData = from_json_bytes(&bytes).unwrap();

        assert_eq!(data, decoded);
    }

    #[test]
    fn test_json_string() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let json = to_json_string(&data).unwrap();
        assert!(json.contains("test"));
        assert!(json.contains("42"));

        let decoded: TestData = from_json_string(&json).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_binary_serialization() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let bytes = to_binary_bytes(&data).unwrap();
        let decoded: TestData = from_binary_bytes(&bytes).unwrap();

        assert_eq!(data, decoded);
    }

    #[test]
    fn test_binary_payload() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let payload = to_binary_payload(&data).unwrap();
        assert_eq!(
            payload.get_metadata_string("encoding"),
            Some("bincode".to_string())
        );

        let decoded: TestData = from_binary_payload(&payload).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_payload_builder() {
        let data = TestData {
            name: "builder".to_string(),
            value: 100,
        };

        let payload = PayloadBuilder::new()
            .json(&data)
            .unwrap()
            .metadata("version", "1.0")
            .metadata("custom", "value")
            .build();

        assert_eq!(
            payload.get_metadata_string("version"),
            Some("1.0".to_string())
        );
        assert_eq!(
            payload.get_metadata_string("custom"),
            Some("value".to_string())
        );

        let decoded: TestData = from_payload(&payload).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_payload_builder_string() {
        let payload = PayloadBuilder::new()
            .string("Hello, World!")
            .metadata("author", "test")
            .build();

        let text = String::from_utf8(payload.data.clone()).unwrap();
        assert_eq!(text, "Hello, World!");
        assert_eq!(
            payload.get_metadata_string("author"),
            Some("test".to_string())
        );
    }

    #[test]
    fn test_payload_builder_bytes() {
        let bytes = vec![1, 2, 3, 4, 5];
        let payload = PayloadBuilder::new()
            .bytes(bytes.clone())
            .metadata("type", "binary")
            .build();

        assert_eq!(payload.data, bytes);
        assert_eq!(
            payload.get_metadata_string("type"),
            Some("binary".to_string())
        );
    }

    #[test]
    fn test_empty_payload() {
        let payload = Payload::default();
        assert!(payload.is_empty());
        assert_eq!(payload.len(), 0);
    }

    #[test]
    fn test_payload_metadata() {
        let payload = Payload::new_data(vec![1, 2, 3])
            .with_metadata("key1", "value1")
            .with_metadata("key2", b"value2".to_vec());

        assert_eq!(
            payload.get_metadata_string("key1"),
            Some("value1".to_string())
        );
        assert_eq!(payload.get_metadata("key2"), Some(b"value2".as_slice()));
    }

    #[cfg(feature = "compression")]
    #[test]
    fn test_compression() {
        let data = b"Hello, World!".repeat(100);
        let compressed = compress_bytes(&data).unwrap();

        // Repetitive input must shrink.
        assert!(compressed.len() < data.len());

        let decompressed = decompress_bytes(&compressed).unwrap();
        assert_eq!(data.to_vec(), decompressed);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn test_compressed_payload() {
        let data = TestData {
            name: "compressed".to_string(),
            value: 999,
        };

        let payload = to_compressed_payload(&data).unwrap();
        assert_eq!(
            payload.get_metadata_string("compressed"),
            Some("true".to_string())
        );

        let decoded: TestData = from_compressed_payload(&payload).unwrap();
        assert_eq!(data, decoded);
    }
}
