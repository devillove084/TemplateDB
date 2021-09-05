use crate::debra::reclaim::internal::Store;
use crate::debra::reclaim::pointer::Marked;
use crate::debra::reclaim::{Owned, Reclaim, Shared, Unlinked, Unprotected, Unsigned};

impl<T, R: Reclaim, N: Unsigned> Store for Owned<T, R, N> {
    type Reclaimer = R;
}

impl<T, R: Reclaim, N: Unsigned> Store for Option<Owned<T, R, N>> {
    type Reclaimer = R;
}

impl<T, R: Reclaim, N: Unsigned> Store for Marked<Owned<T, R, N>> {
    type Reclaimer = R;
}

impl<'g, T, R: Reclaim, N: Unsigned> Store for Shared<'g, T, R, N> {
    type Reclaimer = R;
}

impl<'g, T, R: Reclaim, N: Unsigned> Store for Option<Shared<'g, T, R, N>> {
    type Reclaimer = R;
}

impl<'g, T, R: Reclaim, N: Unsigned> Store for Marked<Shared<'g, T, R, N>> {
    type Reclaimer = R;
}

impl<T, R: Reclaim, N: Unsigned> Store for Unlinked<T, R, N> {
    type Reclaimer = R;
}

impl<T, R: Reclaim, N: Unsigned> Store for Option<Unlinked<T, R, N>> {
    type Reclaimer = R;
}

impl<T, R: Reclaim, N: Unsigned> Store for Marked<Unlinked<T, R, N>> {
    type Reclaimer = R;
}

impl<T, R: Reclaim, N: Unsigned> Store for Unprotected<T, R, N> {
    type Reclaimer = R;
}

impl<T, R: Reclaim, N: Unsigned> Store for Option<Unprotected<T, R, N>> {
    type Reclaimer = R;
}

impl<T, R: Reclaim, N: Unsigned> Store for Marked<Unprotected<T, R, N>> {
    type Reclaimer = R;
}
