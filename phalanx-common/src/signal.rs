use tokio::signal;

pub async fn wait_for_shutdown_signal() {
    signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}
