use std::env::var;
use std::fs::read_dir;
use std::path::Path;

fn main() {
    let ortools_var = var("ORTOOLS_PATH").unwrap();
    let ortools = Path::new(&ortools_var);
    cxx_build::bridge("src/segment_lower_estimator.rs")
        .compiler("g++")
        .include(ortools)
        .include(ortools.join("build"))
        .includes(read_dir(ortools.join("build").join("_deps")).unwrap().map(|x| x.unwrap().path()))
        .file("src/multiknapsack.cpp")
        .flag("-std=c++20")
        .compile("multiknapsack");
    cxx_build::bridge("src/lp_lower_estimator.rs")
        .compiler("g++")
        .include(ortools)
        .include(ortools.join("build"))
        .includes(read_dir(ortools.join("build").join("_deps")).unwrap().map(|x| x.unwrap().path()))
        .file("src/lp_lower_cplex.cpp")
        .flag("-std=c++20")
        .compile("lp_lower_cplex");
    cxx_build::bridge("src/benders_estimator.rs")
        .compiler("g++")
        .include(ortools)
        .include(ortools.join("build"))
        .includes(read_dir(ortools.join("build").join("_deps")).unwrap().map(|x| x.unwrap().path()))
        .file("src/benders.cpp")
        .flag("-std=c++20")
        .compile("benders");

    println!("cargo:rustc-link-search={}", ortools.join("build").join("lib").display());
    println!("cargo:rustc-link-lib=ortools");
    println!("cargo:rerun-if-changed=src/benders.cpp");
}
