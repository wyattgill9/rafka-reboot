use zerocopy::{Immutable, IntoBytes};

#[derive(IntoBytes, Immutable)]
#[repr(C)]
pub struct Message {
    
}
