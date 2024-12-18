use super::compiler_queues::MsgToCompilers;
use super::configs::ConfigFile;
use tokio::sync::mpsc;

pub struct GlobalVars {
    pub configfile: ConfigFile,
    pub to_compiler: mpsc::Sender<MsgToCompilers>,
}

impl GlobalVars {
    pub fn new(configfile: ConfigFile) -> (GlobalVars, mpsc::Receiver<MsgToCompilers>) {
        create_globs(configfile)
    }
}

pub fn create_globs(configfile: ConfigFile) -> (GlobalVars, mpsc::Receiver<MsgToCompilers>) {
    let (to_compiler, receiver_compiler) = mpsc::channel::<MsgToCompilers>(20000);

    let globs = GlobalVars {
        configfile,
        to_compiler,
    };

    (globs, receiver_compiler)
}
