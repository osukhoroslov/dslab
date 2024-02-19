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
}

#[test]
fn test_launching() {
    launch(&["--tasks-count", "1000"]);
}
