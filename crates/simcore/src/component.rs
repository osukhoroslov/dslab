use std::{cmp::Ordering, fmt::Display};

use num_rational::Rational64;
use serde::{Serialize, Serializer};

////////////////////////////////////////////////////////////////////////

pub type Id = u32;

////////////////////////////////////////////////////////////////////////

type FractionalImpl = Rational64;

custom_derive! {
    #[derive(Copy, Clone, NewtypeDebug, NewtypeDeref, NewtypeDerefMut, NewtypeAdd, NewtypeSub, NewtypeMul, NewtypeDiv, NewtypeAddAssign, NewtypeSubAssign, NewtypeMulAssign, NewtypeDivAssign, NewtypeNeg,)]
    pub struct Fractional(FractionalImpl);
}

impl Serialize for Fractional {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
        // let fp = *self.0.numer() as f64 / *self.0.denom() as f64;
        // serializer.serialize_f64(fp)
    }
}

impl Display for Fractional {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fp = *self.0.numer() as f64 / *self.0.denom() as f64;
        f.serialize_f64(fp)
    }
}

impl PartialOrd for Fractional {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Fractional {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap()
    }
}

impl PartialEq for Fractional {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Fractional {}

impl Fractional {
    pub fn from_integer(value: i64) -> Self {
        Self(FractionalImpl::from_integer(value))
    }

    pub fn zero() -> Self {
        Self::from_integer(0)
    }

    pub fn one() -> Self {
        Self::from_integer(1)
    }
}
