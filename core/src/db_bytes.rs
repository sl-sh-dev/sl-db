//! Contains trait needed to serialize/deserlize DB keys and values.

use crate::error::deserialize::DeserializeError;
use crate::error::serialize::SerializeError;

/// Trait that all key and values types must implement to convert to from bytes for the DB.
pub trait DbBytes<T> {
    /// Serialize the type into buffer.
    /// Buffer is expected to contain exactly the serialized type and nothing more.
    /// Implementations can make no assumptions about the state of the buffer passed in.
    /// Resizing the buffer is expected (why it is a Vec not a slice) and may already have sufficient
    /// capacity.
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError>;

    /// Deserialize a byte slice back into the type or error out.
    fn deserialize(buffer: &[u8]) -> Result<T, DeserializeError>;
}

impl DbBytes<String> for String {
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError> {
        let bytes = self.as_bytes();
        buffer.resize(bytes.len(), 0);
        buffer.copy_from_slice(bytes);
        Ok(())
    }

    fn deserialize(buffer: &[u8]) -> Result<String, DeserializeError> {
        Ok(String::from_utf8_lossy(buffer).to_string())
    }
}

impl DbBytes<u8> for u8 {
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError> {
        buffer.resize(1, 0);
        buffer.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }

    fn deserialize(buffer: &[u8]) -> Result<u8, DeserializeError> {
        let mut buf = [0_u8; 1];
        buf.copy_from_slice(buffer);
        Ok(Self::from_le_bytes(buf))
    }
}

impl DbBytes<u16> for u16 {
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError> {
        buffer.resize(2, 0);
        buffer.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }

    fn deserialize(buffer: &[u8]) -> Result<u16, DeserializeError> {
        let mut buf = [0_u8; 2];
        buf.copy_from_slice(buffer);
        Ok(Self::from_le_bytes(buf))
    }
}

impl DbBytes<u32> for u32 {
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError> {
        buffer.resize(4, 0);
        buffer.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }

    fn deserialize(buffer: &[u8]) -> Result<u32, DeserializeError> {
        let mut buf = [0_u8; 4];
        buf.copy_from_slice(buffer);
        Ok(Self::from_le_bytes(buf))
    }
}

impl DbBytes<u64> for u64 {
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError> {
        buffer.resize(8, 0);
        buffer.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }

    fn deserialize(buffer: &[u8]) -> Result<u64, DeserializeError> {
        let mut buf = [0_u8; 8];
        buf.copy_from_slice(buffer);
        Ok(Self::from_le_bytes(buf))
    }
}

/// Allow raw bytes to be used as a value.
impl DbBytes<Vec<u8>> for Vec<u8> {
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError> {
        buffer.resize(self.len(), 0);
        buffer.copy_from_slice(self);
        Ok(())
    }

    fn deserialize(buffer: &[u8]) -> Result<Vec<u8>, DeserializeError> {
        let mut v = vec![0_u8; buffer.len()];
        v.copy_from_slice(buffer);
        Ok(v)
    }
}
