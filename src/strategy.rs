use log::info;
use num_bigint::BigUint;
use num_traits::cast::FromPrimitive;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time;
use vega_protobufs::vega::{instrument::Product, Market};
use vega_protobufs::vega::{Asset, Position};
use vega_wallet_client::WalletClient;

use crate::{binance_ws::RefPrice, vega_store::VegaStore};

pub async fn start(
    clt: WalletClient,
    pubkey: String,
    market: String,
    store: Arc<Mutex<VegaStore>>,
    rp: Arc<Mutex<RefPrice>>,
) {
    // let f: f64 = 2131231231231231231238123712312741823.1231231231231;
    // let big = BigUint::from_f64(f).unwrap();
    // println!("{:?}", big);

    // just loop forever, waiting for user interupt
    let mut interval = time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                interval.reset();
                run_strategy(&clt, pubkey.clone(), market.clone(), store.clone(), rp.clone());
            }
        }
    }
}

fn run_strategy(
    clt: &WalletClient,
    pubkey: String,
    market: String,
    store: Arc<Mutex<VegaStore>>,
    rp: Arc<Mutex<RefPrice>>,
) {
    info!("executing trading strategy...");
    let mkt = store.lock().unwrap().get_market();
    let asset = store.lock().unwrap().get_asset(get_asset(&mkt));

    info!(
        "updating quotes for {}",
        mkt.tradable_instrument
            .as_ref()
            .unwrap()
            .instrument
            .as_ref()
            .unwrap()
            .name
    );

    let d = Decimals::new(&mkt);

    let (best_bid, best_ask) = rp.lock().unwrap().get();
    info!(
        "new reference prices: bestBid({}), bestAsk({})",
        best_bid, best_ask
    );

    let (open_volume, aep) =
        volume_and_average_entry_price(&d, &store.lock().unwrap().get_position());
}

// return vol, aep
fn volume_and_average_entry_price(d: &Decimals, pos: &Option<Position>) -> (f64, f64) {
    if let Some(p) = pos {
        let vol = p.open_volume as f64;
        let aep = p.average_entry_price.parse::<f64>().unwrap();
        return (
            d.from_market_position_precision(vol),
            d.from_market_price_precision(aep),
        );
    }

    return (0., 0.);
}

fn get_asset(mkt: &Market) -> String {
    match mkt
        .clone()
        .tradable_instrument
        .unwrap()
        .instrument
        .unwrap()
        .product
        .unwrap()
    {
        Product::Future(f) => f.settlement_asset,
        _ => panic!("unsupported product"),
    }
}

struct Decimals {
    position_factor: f64,
    price_factor: f64,
}

impl Decimals {
    fn new(mkt: &Market) -> Decimals {
        return Decimals {
            position_factor: (10_f64).powf(mkt.position_decimal_places as f64),
            price_factor: (10_f64).powf(mkt.decimal_places as f64),
        };
    }

    fn from_market_price_precision(&self, price: f64) -> f64 {
        return price / self.price_factor;
    }

    fn from_market_position_precision(&self, position: f64) -> f64 {
        return position / self.position_factor;
    }

    fn to_market_price_precision(&self, price: f64) -> f64 {
        return price * self.price_factor;
    }

    fn to_market_position_precision(&self, position: f64) -> f64 {
        return position * self.position_factor;
    }
}
