macro_rules! __log {
    ($level:tt,$($arg:tt)+) => {
        println!("[{}]\t{}", $level, format!($($arg)*))
    };
}

macro_rules! warn {
    ($($arg:tt)+) => {
        __log!("WARN", $($arg)*)
    };
}

macro_rules! info {
    ($($arg:tt)+) => {
        __log!("INFO", $($arg)*)
    };
}

macro_rules! error {
    ($($arg:tt)+) => {
        __log!("ERROR", $($arg)*)
    };
}

macro_rules! debug {
    ($($arg:tt)+) => {
        #[cfg(feature = "debug")]
        eprintln!("[DEBUG]\t{}:{} tid={:?} {}", file!(), line!(), std::thread::current().id(),
            format!($($arg)*))
    };
}
