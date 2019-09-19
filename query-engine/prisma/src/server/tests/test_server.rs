use std::{thread::{self, JoinHandle}, time::Duration};
use hyper::{Client, Uri, client::HttpConnector};
use async_macros::select;
use async_std::task;
use futures::{future::{self, BoxFuture, FutureExt, TryFutureExt}, channel::oneshot::{self, Sender}};
use crate::{server::HttpServer, PrismaResult};
use tokio_alpha::runtime::Runtime;

pub type ServerAddress = (&'static str, u16);
static ADDR: ServerAddress = ("localhost", 4466);

pub struct TestServer {
    handle: JoinHandle<()>,
    kill_tx: Sender<()>,
    runtime: Runtime,
}

pub fn with_test_setup<F>(datamodel: &str, f: F)
where
    F: FnOnce((ServerAddress, &Runtime)) -> ()
{
    let server = TestServer::start();
    f((ADDR, &server.runtime));
    server.stop();
}

impl TestServer {
    pub fn start() -> Self {
        let (kill_tx, kill_rx) = oneshot::channel::<()>();

        let handle = thread::spawn(move || {
            let server_fut = async {
                if let Err(e) = HttpServer::run(ADDR, false).await {
                    error!("{:?}", e);
                }
            }.boxed();

            let rx_fut = async {
                if let Err(e) = kill_rx.await {
                    error!("{:?}", e);
                }
            }.boxed();

            let fut = select!(server_fut, rx_fut).then(|_| future::ok(()));

            tokio::run(fut.boxed().compat())
        });

        let runtime = Runtime::new().unwrap();
        runtime.block_on(Self::is_running()).unwrap();

        Self {
            handle,
            kill_tx,
            runtime,
        }
    }

    pub fn stop(self) {
        self.kill_tx.send(()).unwrap();
        self.handle.join().unwrap();
    }

    fn is_running() -> BoxFuture<'static, Result<(), ()>> {
        async move {
            task::sleep(Duration::from_millis(50)).await;
            let uri: Uri = format!("http://{}:{}", ADDR.0, ADDR.1).parse().unwrap();
            let result = Client::new().get(uri).await;

            match dbg!(result) {
                Ok(response) => {
                    if !response.status().is_success() {
                        Self::is_running().await?;
                    }
                }
                Err(_) => {
                    Self::is_running().await?;
                }
            }

            Ok(())
        }.boxed()
    }
}
