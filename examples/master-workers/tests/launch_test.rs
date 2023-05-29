use std::process::Command;

#[test]
fn test_launching() {
    let output = Command::new("cargo")
        .args(&["run", "--", "--task-count", "2000"])
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("OUTPUT: {}", stdout);
    println!("STDERR: {}", stderr);

    assert!(output.status.success());
}
