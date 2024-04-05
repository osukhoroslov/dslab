// Custom waker creation logic to avoid Send+Sync requirement.
//
// Based on waker-related code from the futures crate:
// https://github.com/rust-lang/futures-rs/tree/master/futures-task/src

use std::rc::Rc;
use std::task::{RawWaker, RawWakerVTable, Waker};

use core::mem::ManuallyDrop;
use futures::task::WakerRef;

// A way of waking up a specific task.
// By implementing this trait, types that are expected to be wrapped in Rc can be converted into Waker objects.
// The waker is used to signal executor that a task is ready to be polled again.
pub(super) trait RcWake {
    // Indicates that the associated task is ready to make progress and should be polled.
    fn wake(self: Rc<Self>) {
        Self::wake_by_ref(&self)
    }

    // Indicates that the associated task is ready to make progress and should be polled.
    // This function is similar to wake(), but must not consume the provided data pointer.
    fn wake_by_ref(rc_self: &Rc<Self>);
}

// Creates a reference to a Waker from a reference to Rc<impl RcWake>.
// The resulting Waker will call RcWake::wake if awoken.
pub(super) fn waker_ref<W>(wake: &Rc<W>) -> WakerRef<'_>
where
    W: RcWake + 'static,
{
    // simply copy the pointer instead of using Rc::into_raw,
    // as we don't actually keep a refcount by using ManuallyDrop
    let ptr = Rc::as_ptr(wake).cast::<()>();

    let waker = ManuallyDrop::new(unsafe { Waker::from_raw(RawWaker::new(ptr, waker_vtable::<W>())) });
    WakerRef::new_unowned(waker)
}

fn waker_vtable<W: RcWake + 'static>() -> &'static RawWakerVTable {
    &RawWakerVTable::new(
        clone_rc_raw::<W>,
        wake_rc_raw::<W>,
        wake_by_ref_rc_raw::<W>,
        drop_rc_raw::<W>,
    )
}

#[allow(clippy::redundant_clone)] // The clone here isn't actually redundant.
unsafe fn increase_refcount<T: RcWake + 'static>(data: *const ()) {
    // Retain Rc, but don't touch refcount by wrapping in ManuallyDrop
    let rc = ManuallyDrop::new(unsafe { Rc::<T>::from_raw(data.cast::<T>()) });
    // Now increase refcount, but don't drop new refcount either
    let _rc_clone: ManuallyDrop<_> = rc.clone();
}

unsafe fn clone_rc_raw<T: RcWake + 'static>(data: *const ()) -> RawWaker {
    unsafe { increase_refcount::<T>(data) };
    RawWaker::new(data, waker_vtable::<T>())
}

unsafe fn wake_rc_raw<T: RcWake + 'static>(data: *const ()) {
    let rc: Rc<T> = unsafe { Rc::from_raw(data.cast::<T>()) };
    RcWake::wake(rc);
}

unsafe fn wake_by_ref_rc_raw<T: RcWake + 'static>(data: *const ()) {
    // Retain Rc, but don't touch refcount by wrapping in ManuallyDrop
    let rc = ManuallyDrop::new(unsafe { Rc::<T>::from_raw(data.cast::<T>()) });
    RcWake::wake_by_ref(&rc);
}

unsafe fn drop_rc_raw<T: RcWake + 'static>(data: *const ()) {
    drop(unsafe { Rc::<T>::from_raw(data.cast::<T>()) })
}
