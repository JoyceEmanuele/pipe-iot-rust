use std::fmt::Debug;
use std::future::Future;

/*
Esta biblioteca é para criar threads que são essenciais para o funcionamento do programa.
São criadas 2 threads, uma que vai realmente executar a função e outra que vai ficar monitorando a primeira e finaliza o processo caso a thread finalize.
*/

pub fn run_thread_async<F>(thread_name: String, future: F) -> std::thread::JoinHandle<()>
where
    F: Future + Send + 'static,
    <F as Future>::Output: Debug + Send,
{
    std::thread::spawn(move || {
        let result = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Error creating tokio runtime");
            rt.block_on(future)
        })
        .join();
        crate::LOG.append_log_tag_msg(
            "ERROR",
            &format!("Essential thread finished ({}): {:?}", thread_name, result),
        );
        std::process::exit(2);
    })
}

pub fn run_thread_async_loop<F, T>(thread_name: String, func: F) -> std::thread::JoinHandle<()>
where
    F: Fn() -> T + Send + 'static,
    T: Future + Send + 'static,
    <T as Future>::Output: Debug,
{
    std::thread::spawn(move || {
        let thread_name_clone = thread_name.clone();
        let result = std::thread::spawn(move || loop {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Error creating tokio runtime");
            let result_async = rt.block_on(async { func().await });
            crate::LOG.append_log_tag_msg(
                "ERROR",
                &format!(
                    "Essential thread async task finished ({}): {:?}",
                    thread_name_clone, result_async
                ),
            );
            crate::LOG.append_log_tag_msg("INFO", &format!("Will restart in 60 seconds"));
            std::thread::sleep(std::time::Duration::from_secs(60));
        })
        .join();
        crate::LOG.append_log_tag_msg(
            "ERROR",
            &format!("Essential thread finished ({}): {:?}", thread_name, result),
        );
        std::process::exit(2);
    })
}

pub fn run_thread_async_loop_pars<F, T, P>(
    thread_name: String,
    pars: P,
    func: F,
) -> std::thread::JoinHandle<()>
where
    P: Clone + Send + 'static,
    F: Fn(P) -> T + Send + 'static,
    T: Future + Send + 'static,
    <T as Future>::Output: Debug,
{
    std::thread::spawn(move || {
        let thread_name_clone = thread_name.clone();
        let result = std::thread::spawn(move || loop {
            let pars = pars.clone();
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Error creating tokio runtime");
            let result_async = rt.block_on(async { func(pars).await });
            crate::LOG.append_log_tag_msg(
                "ERROR",
                &format!(
                    "Essential thread async task finished ({}): {:?}",
                    thread_name_clone, result_async
                ),
            );
            crate::LOG.append_log_tag_msg("INFO", &format!("Will restart in 60 seconds"));
            std::thread::sleep(std::time::Duration::from_secs(60));
        })
        .join();
        crate::LOG.append_log_tag_msg(
            "ERROR",
            &format!("Essential thread finished ({}): {:?}", thread_name, result),
        );
        std::process::exit(2);
    })
}

pub fn run_thread<F, T>(thread_name: String, func: F) -> std::thread::JoinHandle<()>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
    F::Output: Debug,
{
    std::thread::spawn(move || {
        let result = std::thread::spawn(func).join();
        crate::LOG.append_log_tag_msg(
            "ERROR",
            &format!("Essential thread finished ({}): {:?}", thread_name, result),
        );
        std::process::exit(2);
    })
}

/*
pub fn run_thread_async<F, T>(thread_name: String, func: F)
where
    F: FnOnce() -> T + Send + 'static,
    T: Future + Send + 'static,
    <T as Future>::Output: Debug,
{
    std::thread::spawn(move|| {
        let result = std::thread::spawn(move|| {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("Error creating tokio runtime");
            rt.block_on(async { func().await })
        }).join();
        println!("Error: essential thread finished ({}): {:?}", thread_name, result);
        std::process::exit(2);
    });
}
*/
