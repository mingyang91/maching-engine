use std::{marker::PhantomData, ptr::NonNull};

pub(super) struct DormantMutRef<'a, T> {
    ptr: NonNull<T>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> DormantMutRef<'a, T> {
    #[allow(dead_code)]
    pub(super) fn new(t: &'a mut T) -> (&'a mut T, Self) {
        let ptr = NonNull::from(t);
        let new_ref = unsafe { &mut *ptr.as_ptr() };
        (
            new_ref,
            Self {
                ptr,
                _marker: PhantomData,
            },
        )
    }

    pub(super) fn new_shared(t: &'a T) -> (&'a T, Self) {
        let ptr = NonNull::from(t);
        let new_ref = unsafe { &*ptr.as_ptr() };
        (
            new_ref,
            Self {
                ptr,
                _marker: PhantomData,
            },
        )
    }

    #[allow(dead_code)]
    pub(super) fn awaken(self) -> &'a mut T {
        unsafe { &mut *self.ptr.as_ptr() }
    }

    pub(super) fn reborrow(&mut self) -> &'a mut T {
        unsafe { &mut *self.ptr.as_ptr() }
    }

    #[allow(dead_code)]
    pub(super) fn reborrow_shared(&self) -> &'a T {
        unsafe { &*self.ptr.as_ptr() }
    }
}
