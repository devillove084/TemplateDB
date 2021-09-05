
#[allow(deprecated)]

use core::fmt;
use core::ops::{Add, Sub};
use core::sync::atomic::{AtomicUsize, Ordering};

const EPOCH_INCREMENT: usize = 2;


#[derive(Copy, Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Epoch(usize);


impl Epoch {
    #[inline]
    pub fn new() -> Self {
        Self(0)
    }


    #[inline]
    pub fn relative_age(self, global_epoch: Epoch) -> Result<PossibleAge, Undetermined> {
        match global_epoch.0.wrapping_sub(self.0) {
            0 => Ok(PossibleAge::SameEpoch),
            2 => Ok(PossibleAge::OneEpoch),
            4 => Ok(PossibleAge::TwoEpochs),
            _ => Err(Undetermined),
        }
    }

    #[inline]
    pub(crate) fn with_epoch(epoch: usize) -> Self {
        debug_assert_eq!(epoch % EPOCH_INCREMENT, 0);
        Self(epoch)
    }

    #[inline]
    pub(crate) fn into_inner(self) -> usize {
        self.0
    }
}


impl Add<usize> for Epoch {
    type Output = Self;

    #[inline]
    fn add(self, rhs: usize) -> Self::Output {
        Self(self.0.wrapping_add(rhs * EPOCH_INCREMENT))
    }
}


impl Sub<usize> for Epoch {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: usize) -> Self::Output {
        Self(self.0.wrapping_sub(rhs * EPOCH_INCREMENT))
    }
}


impl fmt::Display for Epoch {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "epoch {}", self.0 / EPOCH_INCREMENT)
    }
}

pub struct AtomicEpoch(AtomicUsize);

impl AtomicEpoch {
    
    #[inline]
    pub const fn new() -> Self {
        Self(AtomicUsize::new(0))
    }


    #[inline]
    pub fn load(&self, order: Ordering) -> Epoch {
        Epoch(self.0.load(order))
    }


    #[inline]
    pub fn compare_and_swap(&self, current: Epoch, new: Epoch, order: Ordering) -> Epoch {
        Epoch(self.0.compare_and_swap(current.0, new.0, order))
    }
}



impl fmt::Debug for AtomicEpoch {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AtomicEpoch").field("epoch", &self.0.load(Ordering::SeqCst)).finish()
    }
}



impl fmt::Display for AtomicEpoch {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "epoch {}", self.0.load(Ordering::SeqCst) / EPOCH_INCREMENT)
    }
}


#[derive(Debug, Copy, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub enum PossibleAge {
    
    SameEpoch,
    
    OneEpoch,
    
    TwoEpochs,
}


impl fmt::Display for PossibleAge {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PossibleAge::SameEpoch => write!(f, "epoch could be the same"),
            PossibleAge::OneEpoch => write!(f, "epoch could be one epoch old"),
            PossibleAge::TwoEpochs => write!(f, "epoch could be two epochs old"),
        }
    }
}


#[derive(Debug, Default, Copy, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct Undetermined;

impl fmt::Display for Undetermined {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "epoch age is undetermined, but older than two epochs")
    }
}