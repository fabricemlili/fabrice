use chrono::Local;

pub fn log(msg: &str, level: &str) {
    if level.to_lowercase() == "error" {
        eprintln!(
            "{} | {:<8} | {}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            level.to_uppercase(),
            msg
        );
        return;
    }
    println!(
        "{} | {:<8} | {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        level.to_uppercase(),
        msg
    );
}