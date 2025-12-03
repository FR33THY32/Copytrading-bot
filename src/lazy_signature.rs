use std::fmt;
use std::sync::OnceLock;

/// A signature that lazily encodes to base58 string
#[derive(Clone)]
pub struct LazySignature {
    raw: Vec<u8>,
    encoded: OnceLock<String>,
}

impl LazySignature {
    /// Create a new lazy signature from raw bytes
    pub fn new(raw: Vec<u8>) -> Self {
        Self {
            raw,
            encoded: OnceLock::new(),
        }
    }

    /// Get the raw signature bytes
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    /// Get the base58-encoded signature string
    pub fn encoded(&self) -> &str {
        self.encoded
            .get_or_init(|| bs58::encode(&self.raw).into_string())
    }

    /// Convert to a base58 string (consumes self)
    pub fn into_string(self) -> String {
        self.encoded
            .into_inner()
            .unwrap_or_else(|| bs58::encode(&self.raw).into_string())
    }
}

impl fmt::Debug for LazySignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazySignature")
            .field("signature", &self.encoded())
            .finish()
    }
}

impl fmt::Display for LazySignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.encoded())
    }
}

impl PartialEq for LazySignature {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl Eq for LazySignature {}

impl std::hash::Hash for LazySignature {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.raw.hash(state);
    }
}

impl From<Vec<u8>> for LazySignature {
    fn from(raw: Vec<u8>) -> Self {
        Self::new(raw)
    }
}

impl From<&[u8]> for LazySignature {
    fn from(raw: &[u8]) -> Self {
        Self::new(raw.to_vec())
    }
}

impl AsRef<[u8]> for LazySignature {
    fn as_ref(&self) -> &[u8] {
        &self.raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lazy_signature_creation() {
        let raw = vec![1, 2, 3, 4, 5];
        let sig = LazySignature::new(raw.clone());
        assert_eq!(sig.raw(), &raw);
    }

    #[test]
    fn test_lazy_encoding() {
        let raw = vec![1, 2, 3, 4, 5];
        let sig = LazySignature::new(raw.clone());

        // First call should compute the encoding
        let encoded1 = sig.encoded();
        // Second call should return the cached value
        let encoded2 = sig.encoded();

        assert_eq!(encoded1, encoded2);
        assert_eq!(encoded1, bs58::encode(&raw).into_string());
    }

    #[test]
    fn test_equality() {
        let raw1 = vec![1, 2, 3, 4, 5];
        let raw2 = vec![1, 2, 3, 4, 5];
        let raw3 = vec![5, 4, 3, 2, 1];

        let sig1 = LazySignature::new(raw1);
        let sig2 = LazySignature::new(raw2);
        let sig3 = LazySignature::new(raw3);

        assert_eq!(sig1, sig2);
        assert_ne!(sig1, sig3);
    }

    #[test]
    fn test_display() {
        let raw = vec![1, 2, 3, 4, 5];
        let sig = LazySignature::new(raw.clone());
        let expected = bs58::encode(&raw).into_string();

        assert_eq!(format!("{}", sig), expected);
    }

    #[test]
    fn test_into_string() {
        let raw = vec![1, 2, 3, 4, 5];
        let sig = LazySignature::new(raw.clone());
        let expected = bs58::encode(&raw).into_string();

        assert_eq!(sig.into_string(), expected);
    }
}
