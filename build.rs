use std::{env, process::Command};

fn main() {
    if env::var_os("CARGO_FEATURE_PYO3_EMBED").is_none() {
        return;
    }
    let python = env::var("PYO3_PYTHON")
        .or_else(|_| env::var("PYTHON"))
        .unwrap_or_else(|_| "python".to_owned());
    let output = Command::new(python)
        .arg("-c")
        .arg(
            "import sysconfig\n\
             print(sysconfig.get_config_var('LIBDIR') or '')\n\
             print(sysconfig.get_config_var('LDLIBRARY') or sysconfig.get_config_var('LIBRARY') or '')",
        )
        .output();
    let Ok(output) = output else {
        return;
    };
    if !output.status.success() {
        return;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut lines = text.lines();
    let Some(libdir) = lines.next().filter(|line| !line.is_empty()) else {
        return;
    };
    let Some(library) = lines.next().filter(|line| !line.is_empty()) else {
        return;
    };
    let Some(libname) = python_link_name(library) else {
        return;
    };
    println!("cargo:rustc-link-search=native={libdir}");
    println!("cargo:rustc-link-lib=dylib={libname}");
}

fn python_link_name(library: &str) -> Option<String> {
    let trimmed = library.strip_prefix("lib").unwrap_or(library);
    for suffix in [".so", ".a", ".dylib"] {
        if let Some(name) = trimmed.strip_suffix(suffix) {
            return Some(name.to_owned());
        }
    }
    None
}
