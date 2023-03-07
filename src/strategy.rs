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
    // just loop forever, waiting for user interupt
    let mut interval = time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                interval.reset();
                run_strategy(&clt, pubkey.clone(), market.clone(), store.clone(), rp.clone()).await;
            }
        }
    }
}

async fn run_strategy(
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

    let d = Decimals::new(&mkt, &asset);

    let (best_bid, best_ask) = rp.lock().unwrap().get();
    info!(
        "new reference prices: bestBid({}), bestAsk({})",
        best_bid, best_ask
    );

    let (open_volume, aep) =
        volume_and_average_entry_price(&d, &store.lock().unwrap().get_position());

    let balance = get_pubkey_balance(store.clone(), pubkey.clone(), asset.id.clone(), &d);
    info!("pubkey balance: {}", balance);

    let bid_volume = balance * 0.5 - open_volume * aep;
    let offer_volume = balance * 0.5 + open_volume * aep;
    let notional_exposure = (open_volume * aep).abs();
    info!(
        "openvolume({}), entryPrice({}), notionalExposure({})",
        open_volume, aep, notional_exposure,
    );
    info!("bidVolume({}), offerVolume({})", bid_volume, offer_volume);

    use vega_wallet_client::commands::{BatchMarketInstructions, OrderCancellation, Side};

    let mut submissions = get_order_submission(&d, best_bid, Side::Buy, market.clone(), bid_volume);
    submissions.append(&mut get_order_submission(
        &d,
        best_ask,
        Side::Sell,
        market.clone(),
        offer_volume,
    ));
    let batch = BatchMarketInstructions {
        cancellations: vec![OrderCancellation {
            market_id: market.clone(),
            order_id: "".to_string(),
        }],
        amendments: vec![],
        submissions,
    };

    info!("batch submission: {:?}", batch);
    clt.send(batch).await.unwrap();
}

fn get_order_submission(
    d: &Decimals,
    ref_price: f64,
    side: vega_wallet_client::commands::Side,
    market_id: String,
    target_volume: f64,
) -> Vec<vega_wallet_client::commands::OrderSubmission> {
    use vega_wallet_client::commands::{OrderSubmission, OrderType, Side, TimeInForce};

    let size = target_volume / 5. * ref_price;

    fn price_buy(ref_price: f64, f: f64) -> f64 {
        ref_price * (1f64 - (f * 0.002))
    }

    fn price_sell(ref_price: f64, f: f64) -> f64 {
        ref_price * (1f64 + (f * 0.002))
    }

    let price_f: fn(f64, f64) -> f64 = match side {
        Side::Buy => price_buy,
        Side::Sell => price_sell,
        _ => panic!("should never happen"),
    };

    let mut orders: Vec<OrderSubmission> = vec![];
    for i in vec![1, 2, 3, 4, 5].into_iter() {
        let p =
            BigUint::from_f64(d.to_market_price_precision(price_f(ref_price, i as f64))).unwrap();

        orders.push(OrderSubmission {
            market_id: market_id.clone(),
            price: p.to_string(),
            size: d.to_market_position_precision(size) as u64,
            side,
            time_in_force: TimeInForce::Gtc,
            expires_at: 0,
            r#type: OrderType::Limit,
            reference: "VEGA_RUST_MM_SIMPLE".to_string(),
            pegged_order: None,
        });
    }

    return orders;
}

fn get_pubkey_balance(
    store: Arc<Mutex<VegaStore>>,
    pubkey: String,
    asset_id: String,
    d: &Decimals,
) -> f64 {
    d.from_asset_precision(store.lock().unwrap().get_accounts().iter().fold(
        0f64,
        |balance, acc| {
            if acc.asset != asset_id || acc.owner != pubkey {
                balance
            } else {
                balance + acc.balance.parse::<f64>().unwrap()
            }
        },
    ))
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
    }
}

struct Decimals {
    position_factor: f64,
    price_factor: f64,
    asset_factor: f64,
}

impl Decimals {
    fn new(mkt: &Market, asset: &Asset) -> Decimals {
        return Decimals {
            position_factor: (10_f64).powf(mkt.position_decimal_places as f64),
            price_factor: (10_f64).powf(mkt.decimal_places as f64),
            asset_factor: (10_f64).powf(asset.details.as_ref().unwrap().decimals as f64),
        };
    }

    fn from_asset_precision(&self, amount: f64) -> f64 {
        return amount / self.asset_factor;
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
