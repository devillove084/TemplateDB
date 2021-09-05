


pub use self::Aligned64 as CacheAligned;

use core::borrow::{Borrow, BorrowMut};
use core::ops::{Deref, DerefMut};

macro_rules! impl_align {
    ($(struct align($align:expr) $wrapper:ident;)*) => {
        $(
            #[doc = "A thin wrapper type with an alignment of at least $align bytes."]
            #[derive(Copy, Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
            #[repr(align($align))]
            pub struct $wrapper<T>(pub T);

            impl<T> $wrapper<T> {
                
                pub fn get(aligned: &Self) -> &T {
                    &aligned.0
                }
            }

            impl<T> Deref for $wrapper<T> {
                type Target = T;

                #[inline]
                fn deref(&self) -> &Self::Target {
                    &self.0
                }
            }

            impl<T> DerefMut for $wrapper<T> {
                #[inline]
                fn deref_mut(&mut self) -> &mut Self::Target {
                    &mut self.0
                }
            }

            impl<T> AsRef<T> for $wrapper<T> {
                #[inline]
                fn as_ref(&self) -> &T {
                    &self.0
                }
            }

            impl<T> AsMut<T> for $wrapper<T> {
                #[inline]
                fn as_mut(&mut self) -> &mut T {
                    &mut self.0
                }
            }

            impl<T> Borrow<T> for $wrapper<T> {
                #[inline]
                fn borrow(&self) -> &T {
                    &self.0
                }
            }

            impl<T> BorrowMut<T> for $wrapper<T> {
                #[inline]
                fn borrow_mut(&mut self) -> &mut T {
                    &mut self.0
                }
            }
        )*
    };
}

impl_align! {
    struct align(1) Aligned1;
    struct align(2) Aligned2;
    struct align(4) Aligned4;
    struct align(8) Aligned8;
    struct align(16) Aligned16;
    struct align(32) Aligned32;
    struct align(64) Aligned64;
    struct align(128) Aligned128;
    struct align(256) Aligned256;
    struct align(512) Aligned512;
    struct align(1024) Aligned1024;
    struct align(2048) Aligned2048;
    struct align(4096) Aligned4096;
    struct align(0x2000) Aligned8k;
    struct align(0x4000) Aligned16k;
    struct align(0x8000) Aligned32k;
    struct align(0x10000) Aligned64k;
    struct align(0x20000) Aligned128k;
    struct align(0x40000) Aligned256k;
    struct align(0x80000) Aligned512k;
    struct align(0x100000) Aligned1M;
    struct align(0x200000) Aligned2M;
    struct align(0x400000) Aligned4M;
    struct align(0x800000) Aligned8M;
    struct align(0x1000000) Aligned16M;
    struct align(0x2000000) Aligned32M;
    struct align(0x4000000) Aligned64M;
    struct align(0x8000000) Aligned128M;
    struct align(0x10000000) Aligned256M;
    struct align(0x10000000) Aligned512M;
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;

    #[test]
    fn alignments() {
        assert_eq!(mem::align_of::<Aligned8<u8>>(), 8);
        assert_eq!(mem::align_of::<Aligned16<u8>>(), 16);
        assert_eq!(mem::align_of::<Aligned32<u8>>(), 32);
        assert_eq!(mem::align_of::<Aligned64<u8>>(), 64);
        assert_eq!(mem::align_of::<Aligned128<u8>>(), 128);
        assert_eq!(mem::align_of::<Aligned256<u8>>(), 256);
        assert_eq!(mem::align_of::<Aligned512<u8>>(), 512);
        assert_eq!(mem::align_of::<Aligned1024<u8>>(), 1024);
        assert_eq!(mem::align_of::<Aligned2048<u8>>(), 2048);
        assert_eq!(mem::align_of::<Aligned4096<u8>>(), 4096);
    }

    #[test]
    fn construct_and_deref() {
        let value = Aligned8(255u8);
        assert_eq!(*value, 255);

        let value = CacheAligned(1u8);
        assert_eq!(*value, 1);
    }
}
