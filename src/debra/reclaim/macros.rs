



macro_rules! impl_trait {
    ($self:ident) => {
        type Pointer = Self;
        type Item = T;
        type MarkBits = N;

        #[inline]
        fn as_marked_ptr(&self) -> crate::debra::reclaim::pointer::MarkedPtr<T, N> {
            self.inner.into_marked_ptr()
        }

        #[inline]
        fn into_marked_ptr(self) -> crate::debra::reclaim::pointer::MarkedPtr<Self::Item, Self::MarkBits> {
            self.into_marked_non_null().into_marked_ptr()
        }

        #[inline]
        fn marked($self: Self, tag: usize) -> crate::debra::reclaim::pointer::Marked<Self::Pointer> {
            let inner = $self.inner.with_tag(tag);
            crate::debra::reclaim::pointer::Marked::Value(Self { inner, _marker: PhantomData })
        }

        #[inline]
        fn unmarked($self: Self) -> Self {
            let inner = $self.inner.clear_tag();
            Self { inner, _marker: PhantomData }
        }

        #[inline]
        fn decompose($self: Self) -> (Self, usize) {
            let (inner, tag) = $self.inner.decompose();
            core::mem::forget($self);
            ( Self { inner: crate::debra::reclaim::pointer::MarkedNonNull::from(inner), _marker: PhantomData }, tag)
        }

        #[inline]
        unsafe fn from_marked_ptr(
            marked: crate::debra::reclaim::pointer::MarkedPtr<Self::Item, Self::MarkBits>
        ) -> Self
        {
            debug_assert!(!marked.is_null());
            Self { inner: MarkedNonNull::new_unchecked(marked), _marker: PhantomData}
        }

        #[inline]
        unsafe fn from_marked_non_null(
            marked: crate::debra::reclaim::pointer::MarkedNonNull<Self::Item, Self::MarkBits>
        ) -> Self
        {
            Self { inner: marked, _marker: PhantomData }
        }
    };
}

macro_rules! impl_inherent {
    ($self:ident) => {
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        #[inline]
        pub fn none() -> Option<Self> {
            None
        }

        
        
        #[inline]
        pub fn null() -> crate::debra::reclaim::pointer::Marked<Self> {
            Marked::Null(0)
        }

        
        
        #[inline]
        pub fn null_with_tag(tag: usize) -> crate::debra::reclaim::pointer::Marked<Self> {
            Marked::Null(tag)
        }

        
        
        
        
        #[inline]
        pub fn compose($self: Self, tag: usize) -> Self {
            let inner = $self.inner;
            core::mem::forget($self);
            Self { inner: inner.with_tag(tag), _marker: PhantomData }
        }
    };
}
