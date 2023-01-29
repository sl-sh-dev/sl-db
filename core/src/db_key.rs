//! Contains the trait that must be imple,ented for anything acting as a key.

use std::fmt::Debug;
use std::hash::Hash;

/// Required trait for a key.  Note that setting KSIZE to 0 indicates a variable sized key.
pub trait DbKey<const KSIZE: u16>: Eq + Hash + Debug {
    /// Defines the key size for fixed size keys (will be 0 for variable sized keys).
    const KEY_SIZE: u16 = KSIZE;

    /// True if this DB has a fixed size key.
    #[inline(always)]
    fn is_fixed_key_size() -> bool {
        Self::KEY_SIZE > 0
    }

    /// True if this DB has a variable size key.
    /// Variable sized keys require key sizes to be saved in the DB.
    #[inline(always)]
    fn is_variable_key_size() -> bool {
        Self::KEY_SIZE == 0
    }
}

impl DbKey<0> for String {}
impl DbKey<8> for u64 {}
/// Allow raw bytes to be used as a key.
impl DbKey<0> for Vec<u8> {}
