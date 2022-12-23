pub type TestResult = Result<bool, String>;

pub struct Test<T> {
    name: String,
    func: fn(&T) -> TestResult,
    config: T,
}

pub struct TestSuite<T> {
    tests: Vec<Test<T>>,
}

impl<T> TestSuite<T> {
    pub fn new() -> Self {
        Self { tests: Vec::new() }
    }

    pub fn add(&mut self, name: &str, f: fn(&T) -> TestResult, config: T) {
        self.tests.push(Test {
            name: name.to_string(),
            func: f,
            config,
        });
    }

    pub fn run(&mut self) {
        let mut passed_count = 0;
        let mut failed_tests = Vec::new();
        for test in &self.tests {
            println!("\n--- {} ---\n", test.name);
            match (test.func)(&test.config) {
                Ok(_) => {
                    println!("\nPASSED\n");
                    passed_count += 1;
                }
                Err(e) => {
                    println!("\nFAILED: {}\n", e);
                    failed_tests.push((&test.name, e));
                }
            }
        }
        println!("-------------------------------------------------------------------------------");
        println!("\nPassed {} from {} tests\n", passed_count, self.tests.len());
        if !failed_tests.is_empty() {
            println!("Failed tests:");
            for (test, e) in failed_tests {
                println!("- {}: {}", test, e);
            }
            println!();
            std::process::exit(1);
        } else {
            std::process::exit(0);
        }
    }

    pub fn run_test(&mut self, name: &str) {
        for test in &self.tests {
            if test.name == name {
                println!("\n--- {} ---\n", test.name);
                match (test.func)(&test.config) {
                    Ok(_) => println!("\nPASSED\n"),
                    Err(e) => println!("\nFAILED: {}\n", e),
                }
            }
        }
    }
}

impl<T> Default for TestSuite<T> {
    fn default() -> Self {
        TestSuite::new()
    }
}
