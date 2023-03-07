use clap::Parser;
use log::info;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time;
use vega_protobufs::datanode::api::v2::trading_data_service_client::TradingDataServiceClient;
use vega_store::update_forever;

mod api;
mod binance_ws;
mod strategy;
mod vega_store;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Port of the http API
    #[arg(long, default_value_t = 8080)]
    port: u16,
    /// A vega grpc node address
    #[arg(long, default_value_t = String::from("tcp://n11.testnet.vega.xyz:3007"))]
    vega_grpc_url: String,
    /// A vega wallet service address
    #[arg(long, default_value_t = String::from("http://127.0.0.1:1789"))]
    wallet_url: String,
    /// Binance websocket url
    #[arg(long, default_value_t = String::from("wss://stream.binance.com:443/ws"))]
    binance_ws_url: String,
    /// An API token for the vega wallet service
    #[arg(long)]
    wallet_token: String,
    /// A Vega public key to be used to submit transactions
    #[arg(long)]
    wallet_pubkey: String,
    /// An ID of a market in Vega
    #[arg(long)]
    vega_market: String,
    /// An Binance market symbol
    #[arg(long)]
    binance_market: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let cli = Cli::parse();

    info!("connecting with the go wallet service");
    let wclt = vega_wallet_client::WalletClient::new(
        &cli.wallet_url,
        &cli.wallet_token,
        &cli.wallet_pubkey,
    )
    .await?;
    info!("connection with the go wallet service successful");

    let rp = Arc::new(Mutex::new(binance_ws::RefPrice::new()));

    tokio::spawn(binance_ws::start(
        cli.binance_ws_url.clone(),
        cli.binance_market.clone(),
        rp.clone(),
    ));

    let addr = cli.vega_grpc_url.clone();
    let mut tdclt = TradingDataServiceClient::connect(addr).await?;

    let vstore = Arc::new(Mutex::new(
        vega_store::VegaStore::new(&mut tdclt, &*cli.vega_market, &*cli.wallet_pubkey).await?,
    ));

    update_forever(
        vstore.clone(),
        tdclt,
        &*cli.vega_market,
        &*cli.wallet_pubkey,
    );

    tokio::spawn(api::start(cli.port, vstore.clone(), rp.clone()));

    tokio::spawn(strategy::start(
        wclt,
        cli.wallet_pubkey.clone(),
        cli.vega_market.clone(),
        vstore.clone(),
        rp.clone(),
    ));

    // just loop forever, waiting for user interupt
    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                interval.reset();
            }
        }
    }
}
