#![allow(unused_braces, dead_code, unused_imports)]

use std::{
    ffi::{CStr, c_char, c_void, c_int},
    mem
};

unsafe extern "C" {
    fn add_c(a: c_int, b: c_int) -> c_int;
}

pub fn add(a: i32, b: i32) -> i32 {
    unsafe { return add_c(a as c_int, b as c_int); }
}
