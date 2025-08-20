fn main() {
    let mut build = cc::Build::new();

    build.include("./include");

    // compile every .c file in ./csrc
    for entry in std::fs::read_dir("csrc").unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) == Some("c") {
            build.file(path);
        }
    }

    build.compile("bridge");
}
