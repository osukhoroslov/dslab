use std::process::Command;

fn launch(args: &[&str]) {
    let output = Command::new("cargo")
        .args(&["run", "--"])
        .args(args)
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("OUTPUT: {}", stdout);
    println!("STDERR: {}", stderr);

    assert!(output.status.success());
    assert!(!stderr.to_lowercase().contains("error"));
}

#[test]
fn test_default() {
    launch(&["--proc-count", "1000", "--peer-count", "100", "--iterations", "100"]);
}

#[test]
fn test_with_network() {
    launch(&[
        "--proc-count",
        "1000",
        "--peer-count",
        "100",
        "--iterations",
        "100",
        "--use-network",
    ]);
}
