//! Custom task waker creation to avoid Send+Sync dependency.
//!
//! Original implementation from crate futures:
//! https://github.com/rust-lang/futures-rs/blob/master/futures-task/src/waker.rs

use std::{
    sync::Arc,
    task::{RawWaker, RawWakerVTable, Waker},
};

use core::mem::ManuallyDrop;
use futures::task::WakerRef;

pub(super) trait CustomWake {
    fn wake(self: Arc<Self>) {
        Self::wake_by_ref(&self)
    }

    fn wake_by_ref(arc_self: &Arc<Self>);
}

pub(super) fn waker_ref<W>(wake: &Arc<W>) -> WakerRef<'_>
where
    W: CustomWake,
{
    // simply copy the pointer instead of using Arc::into_raw,
    // as we don't actually keep a refcount by using ManuallyDrop.<
    let ptr = Arc::as_ptr(wake).cast::<()>();

    let waker = ManuallyDrop::new(unsafe { Waker::from_raw(RawWaker::new(ptr, waker_vtable::<W>())) });
    WakerRef::new_unowned(waker)
}

pub(super) fn waker_vtable<W: CustomWake>() -> &'static RawWakerVTable {
    &RawWakerVTable::new(
        clone_arc_raw::<W>,
        wake_arc_raw::<W>,
        wake_by_ref_arc_raw::<W>,
        drop_arc_raw::<W>,
    )
}

#[allow(clippy::redundant_clone)] // The clone here isn't actually redundant.
unsafe fn increase_refcount<T: CustomWake>(data: *const ()) {
    // Retain Arc, but don't touch refcount by wrapping in ManuallyDrop
    let arc = ManuallyDrop::new(Arc::<T>::from_raw(data.cast::<T>()));
    // Now increase refcount, but don't drop new refcount either
    let _arc_clone: ManuallyDrop<_> = arc.clone();
}

// used by `waker_ref`
unsafe fn clone_arc_raw<T: CustomWake>(data: *const ()) -> RawWaker {
    increase_refcount::<T>(data);
    RawWaker::new(data, waker_vtable::<T>())
}

unsafe fn wake_arc_raw<T: CustomWake>(data: *const ()) {
    let arc: Arc<T> = Arc::from_raw(data.cast::<T>());
    CustomWake::wake(arc);
}

// used by `waker_ref`
unsafe fn wake_by_ref_arc_raw<T: CustomWake>(data: *const ()) {
    // Retain Arc, but don't touch refcount by wrapping in ManuallyDrop
    let arc = ManuallyDrop::new(Arc::<T>::from_raw(data.cast::<T>()));
    CustomWake::wake_by_ref(&arc);
}

unsafe fn drop_arc_raw<T: CustomWake>(data: *const ()) {
    drop(Arc::<T>::from_raw(data.cast::<T>()))
}