use fastrace::collector::ConsoleReporter;

pub struct FastraceWrapper();

impl FastraceWrapper {
    // todo: config to control more
    pub fn init() {
        fastrace::set_reporter(ConsoleReporter, fastrace::collector::Config::default());
    }
}

impl Drop for FastraceWrapper {
    fn drop(&mut self) {
        fastrace::flush();
    }
}
