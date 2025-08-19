use std::os::raw::c_int;

unsafe extern "C" {
    pub fn add_c(a: c_int, b: c_int) -> c_int;
}

pub fn add(a: i32, b: i32) -> i32 {
    unsafe {
        return add_c(a,b);
    }
}
