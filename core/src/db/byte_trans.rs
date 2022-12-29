//! Some really unsafe code to allow structs that are copy to be written to and read from Files.
//! It can turn the struct in a &[u8] or &mut [u8] to accomplish this.
//! This is not safe at all, especially the &mut [u8] so use with care.

/// Unsafe trait that can be used to turn a struct in to a byte buffer and byte buffer into a struct.
/// This is the old C technique of just reading and writing structs as bytes.
/// As long as the struct is copy then as_bytes is probably fine but building a struct from
/// a byte buffer is rife with potential UB...
/// If you use this you almost certainly want "#[repr(C)]" on the structs.
pub trait ByteTrans<T: ByteTrans<T> + Copy> {
    /// Size of T as a const.
    const SIZE: usize = std::mem::size_of::<T>();

    /// Turn T into a &[u8].
    ///
    /// # Safety
    ///
    /// Turns the underlying (Copy) data structure into a byte slice.  Since this is read
    /// only it should not be dangerous.
    unsafe fn as_bytes(t: &T) -> &[u8] {
        std::slice::from_raw_parts((t as *const T) as *const u8, T::SIZE)
    }

    /// Turn T into a &mut [u8].
    /// This allows direct writes into T is very unsafe.
    ///
    /// # Safety
    ///
    /// This is allows directly writing the bytes that make up a data structure.  This is
    /// very unsafe and could easily violate invariants of the data- use with caution.
    unsafe fn as_bytes_mut(t: &mut T) -> &mut [u8] {
        std::slice::from_raw_parts_mut((t as *mut T) as *mut u8, T::SIZE)
    }
}

impl<T: Copy> ByteTrans<T> for T {}
