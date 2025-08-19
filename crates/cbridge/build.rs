fn main() {
    cc::Build::new()
        .file("include/test/lib.c")
        .include("include/test")
        .compile("test");

    // cc::Build::new()
        // .file("include/test2/lib.c")
    //     .include("include/test2")
    //     .compile("test2");
}
