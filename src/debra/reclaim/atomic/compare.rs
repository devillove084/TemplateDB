use crate::debra::reclaim::internal::Compare;
use crate::debra::reclaim::pointer::Marked;
use crate::debra::reclaim::{Reclaim, Shared, Unlinked, Unprotected, Unsigned};

impl<'g, T, R: Reclaim, N: Unsigned> Compare for Shared<'g, T, R, N> {
    type Reclaimer = R;
    type Unlinked = Unlinked<T, R, N>;
}

impl<'g, T, R: Reclaim, N: Unsigned> Compare for Option<Shared<'g, T, R, N>> {
    type Reclaimer = R;
    type Unlinked = Option<Unlinked<T, R, N>>;
}

impl<'g, T, R: Reclaim, N: Unsigned> Compare for Marked<Shared<'g, T, R, N>> {
    type Reclaimer = R;
    type Unlinked = Marked<Unlinked<T, R, N>>;
}

impl<T, R: Reclaim, N: Unsigned> Compare for Unprotected<T, R, N> {
    type Reclaimer = R;
    type Unlinked = Unlinked<T, R, N>;
}

impl<T, R: Reclaim, N: Unsigned> Compare for Option<Unprotected<T, R, N>> {
    type Reclaimer = R;
    type Unlinked = Option<Unlinked<T, R, N>>;
}

impl<T, R: Reclaim, N: Unsigned> Compare for Marked<Unprotected<T, R, N>> {
    type Reclaimer = R;
    type Unlinked = Marked<Unlinked<T, R, N>>;
}
