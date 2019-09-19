mod test_server;

use test_server::with_test_setup;
use futures::future::{self, FutureExt};
use crate::error::PrismaError;
use test::Bencher;
use hyper::{Client, Uri, client::HttpConnector};

#[bench]
fn test_boom(b: &mut Bencher) {
    test_server::with_test_setup(|(addr, runtime)| {
        let client: Client<HttpConnector> = Client::builder().keep_alive(true).build_http();

        let uri: Uri = format!("http://{}:{}", addr.0, addr.1).parse().unwrap();

        b.iter(|| {
            let reqs = (0..1000).into_iter().map(|_| client.get(uri.clone()));
            runtime.block_on(async {
                future::join_all(reqs).await;
            })
        });

    })
}
