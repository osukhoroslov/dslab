//! Custom task waker creation to avoid Send+Sync dependency.
//!
//! Original implementation from crate futures:
//! https://github.com/rust-lang/futures-rs/blob/master/futures-task/src/waker.rs

use std::{
    rc::Rc,
    task::{RawWaker, RawWakerVTable, Waker},
};

use core::mem::ManuallyDrop;
use futures::task::WakerRef;

pub(super) trait CustomWake {
    fn wake(self: Rc<Self>) {
        Self::wake_by_ref(&self)
    }

    fn wake_by_ref(rc_self: &Rc<Self>);
}

pub(super) fn waker_ref<W>(wake: &Rc<W>) -> WakerRef<'_>
where
    W: CustomWake,
{
    // simply copy the pointer instead of using Rc::into_raw,
    // as we don't actually keep a refcount by using ManuallyDrop.<
    let ptr = Rc::as_ptr(wake).cast::<()>();

    let waker = ManuallyDrop::new(unsafe { Waker::from_raw(RawWaker::new(ptr, waker_vtable::<W>())) });
    WakerRef::new_unowned(waker)
}

pub(super) fn waker_vtable<W: CustomWake>() -> &'static RawWakerVTable {
    &RawWakerVTable::new(
        clone_rc_raw::<W>,
        wake_rc_raw::<W>,
        wake_by_ref_rc_raw::<W>,
        drop_rc_raw::<W>,
    )
}

#[allow(clippy::redundant_clone)] // The clone here isn't actually redundant.
unsafe fn increase_refcount<T: CustomWake>(data: *const ()) {
    // Retain Rc, but don't touch refcount by wrapping in ManuallyDrop
    let rc = ManuallyDrop::new(Rc::<T>::from_raw(data.cast::<T>()));
    // Now increase refcount, but don't drop new refcount either
    let _rc_clone: ManuallyDrop<_> = rc.clone();
}

// used by `waker_ref`
unsafe fn clone_rc_raw<T: CustomWake>(data: *const ()) -> RawWaker {
    increase_refcount::<T>(data);
    RawWaker::new(data, waker_vtable::<T>())
}

unsafe fn wake_rc_raw<T: CustomWake>(data: *const ()) {
    let rc: Rc<T> = Rc::from_raw(data.cast::<T>());
    CustomWake::wake(rc);
}

// used by `waker_ref`
unsafe fn wake_by_ref_rc_raw<T: CustomWake>(data: *const ()) {
    // Retain Rc, but don't touch refcount by wrapping in ManuallyDrop
    let rc = ManuallyDrop::new(Rc::<T>::from_raw(data.cast::<T>()));
    CustomWake::wake_by_ref(&rc);
}

unsafe fn drop_rc_raw<T: CustomWake>(data: *const ()) {
    drop(Rc::<T>::from_raw(data.cast::<T>()))
}
