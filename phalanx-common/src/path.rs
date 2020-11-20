use std::path::{Path, PathBuf};

use dirs;

pub fn expand_tilde<P: AsRef<Path>>(input_path: P) -> Option<PathBuf> {
    let path = input_path.as_ref();
    if !path.starts_with("~") {
        return Some(path.to_path_buf());
    }
    if path == Path::new("~") {
        return dirs::home_dir();
    }
    dirs::home_dir().map(|mut home| {
        if home == Path::new("/") {
            // Corner case: `h` root directory;
            // don't prepend extra `/`, just drop the tilde.
            path.strip_prefix("~").unwrap().to_path_buf()
        } else {
            home.push(path.strip_prefix("~/").unwrap());
            home
        }
    })
}
