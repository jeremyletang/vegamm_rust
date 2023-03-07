use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use log::error;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use crate::{binance_ws::RefPrice, vega_store::VegaStore};

#[derive(Serialize, Deserialize)]
struct Resp {
    best_bid: f64,
    best_ask: f64,
    position: String,
    market: String,
    market_data: String,
    accounts: String,
    orders: String,
    assets: String,
}

async fn handle(
    store: Arc<Mutex<VegaStore>>,
    rp: Arc<Mutex<RefPrice>>,
    _req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    let (bb, ba) = rp.lock().unwrap().get();
    // lazy implementation, none of these implement Serde interface, so just dumping strings
    Ok(Response::new(Body::from(
        serde_json::to_string(&Resp {
            best_bid: bb,
            best_ask: ba,
            position: format!("{:?}", store.lock().unwrap().get_position()),
            accounts: format!("{:?}", store.lock().unwrap().get_accounts()),
            orders: format!("{:?}", store.lock().unwrap().get_orders()),
            market: format!("{:?}", store.lock().unwrap().get_market()),
            market_data: format!("{:?}", store.lock().unwrap().get_market_data()),
            assets: format!("{:?}", store.lock().unwrap().get_assets()),
        })
        .unwrap(),
    )))
}

pub async fn start(port: u16, store: Arc<Mutex<VegaStore>>, rp: Arc<Mutex<RefPrice>>) {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    let make_service = make_service_fn(move |_conn: &AddrStream| {
        let store = store.clone();
        let rp = rp.clone();

        let service = service_fn(move |req| handle(store.clone(), rp.clone(), req));

        async move { Ok::<_, Infallible>(service) }
    });
    let server = Server::bind(&addr).serve(make_service);

    // then run forever...
    if let Err(e) = server.await {
        error!("api server error: {}", e);
    }
}
