use log::info;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::sync::{Arc, Mutex};
use tokio_stream::StreamExt;
use tonic;
use vega_protobufs::{
    datanode::api::v2::{
        trading_data_service_client::TradingDataServiceClient, AccountBalance, AccountFilter,
        GetLatestMarketDataRequest, GetMarketRequest, ListAccountsRequest, ListAssetsRequest,
        ListOrdersRequest, ListPositionsRequest, ObserveAccountsRequest, ObserveMarketsDataRequest,
        ObserveOrdersRequest, ObservePositionsRequest,
    },
    vega::{AccountType, Asset, Market, MarketData, Order, Position},
};

pub struct VegaStore {
    market: Market,
    market_data: MarketData,
    // key = type+asset+market
    accounts: HashMap<String, AccountBalance>,
    // key = order ID
    orders: HashMap<String, Order>,
    position: Option<Position>,
    // key = asset ID
    assets: HashMap<String, Asset>,
}

impl VegaStore {
    pub async fn new(
        clt: &mut TradingDataServiceClient<tonic::transport::Channel>,
        mkt_id: &str,
        pubkey: &str,
    ) -> Result<VegaStore, Error> {
        info!("1");
        let mkt_resp = clt
            .get_market(GetMarketRequest {
                market_id: mkt_id.to_string(),
            })
            .await?;

        info!(
            "market found: {:?}",
            mkt_resp.get_ref().market.as_ref().unwrap().clone(),
        );

        let mkt_data_resp = clt
            .get_latest_market_data(GetLatestMarketDataRequest {
                market_id: mkt_id.to_string(),
            })
            .await?;

        info!(
            "market data found: {:?}",
            mkt_data_resp
                .get_ref()
                .market_data
                .as_ref()
                .unwrap()
                .clone(),
        );

        let pos_resp = clt
            .list_positions(ListPositionsRequest {
                market_id: mkt_id.to_string(),
                party_id: pubkey.to_string(),
                pagination: None,
            })
            .await?;

        let position = match &pos_resp.get_ref().positions {
            Some(p) => match p.edges.len() {
                0 => None,
                1 => p.edges[0].node.clone(),
                _ => unreachable!("cannot have 2 position for the same market"),
            },
            None => None,
        };

        let orders_resp = clt
            .list_orders(ListOrdersRequest {
                party_id: Some(pubkey.to_string()),
                market_id: Some(mkt_id.to_string()),
                live_only: Some(true),
                filter: None,
                date_range: None,
                reference: None,
                pagination: None,
            })
            .await?;

        let mut orders = HashMap::new();
        for o in orders_resp.get_ref().orders.as_ref().unwrap().edges.iter() {
            let order = o.node.as_ref().unwrap();
            orders.insert(order.id.clone(), order.clone());
        }

        let accounts_resp = clt
            .list_accounts(ListAccountsRequest {
                filter: Some(AccountFilter {
                    party_ids: vec![pubkey.to_string()],
                    account_types: vec![],
                    asset_id: "".to_string(),
                    market_ids: vec![],
                }),
                pagination: None,
            })
            .await?;

        let mut accounts = HashMap::new();
        for a in accounts_resp
            .get_ref()
            .accounts
            .as_ref()
            .unwrap()
            .edges
            .iter()
        {
            let account = a.node.as_ref().unwrap();
            accounts.insert(
                format!("{}{}{}", account.r#type, account.asset, account.market_id),
                account.clone(),
            );
        }

        let assets_resp = clt
            .list_assets(ListAssetsRequest {
                asset_id: None,
                pagination: None,
            })
            .await?;

        let mut assets = HashMap::new();
        for a in assets_resp.get_ref().assets.as_ref().unwrap().edges.iter() {
            let asset = a.node.as_ref().unwrap();
            assets.insert(asset.id.clone(), asset.clone());
        }

        return Ok(VegaStore {
            market: mkt_resp.get_ref().market.as_ref().unwrap().clone(),
            market_data: mkt_data_resp
                .get_ref()
                .market_data
                .as_ref()
                .unwrap()
                .clone(),
            assets,
            position,
            orders,
            accounts,
        });
    }

    pub fn get_market(&self) -> Market {
        return self.market.clone();
    }

    pub fn get_asset(&self, id: String) -> Asset {
        return self.assets[&id].clone();
    }

    pub fn get_market_data(&self) -> MarketData {
        return self.market_data.clone();
    }

    pub fn get_position(&self) -> Option<Position> {
        return self.position.clone();
    }

    pub fn get_orders(&self) -> Vec<Order> {
        return self.orders.clone().into_values().collect();
    }

    pub fn get_accounts(&self) -> Vec<AccountBalance> {
        return self.accounts.clone().into_values().collect();
    }

    pub fn get_assets(&self) -> Vec<Asset> {
        return self.assets.clone().into_values().collect();
    }

    pub fn get_account(
        &self,
        typ: AccountType,
        asset: String,
        market: String,
    ) -> Option<AccountBalance> {
        return self
            .accounts
            .get(&format!("{}{}{}", typ as i32, asset, market))
            .and_then(|a| Some(a.clone()));
    }

    pub fn save_market_data(&mut self, md: MarketData) {
        self.market_data = md;
    }

    pub fn save_orders(&mut self, orders: Vec<Order>) {
        use vega_protobufs::vega::order::Status;
        for o in orders.into_iter() {
            if Status::from_i32(o.status).unwrap() != Status::Active {
                self.orders.remove(&o.id);
                continue;
            }
            self.orders.insert(o.id.clone(), o);
        }
    }

    pub fn save_positions(&mut self, positions: Vec<Position>) {
        for p in positions.into_iter() {
            self.position = Some(p);
        }
    }

    pub fn save_accounts(&mut self, accounts: Vec<AccountBalance>) {
        for a in accounts.into_iter() {
            self.accounts
                .insert(format!("{}{}{}", a.r#type, a.asset, a.market_id), a);
        }
    }
}

pub fn update_forever(
    store: Arc<Mutex<VegaStore>>,
    clt: TradingDataServiceClient<tonic::transport::Channel>,
    market: &str,
    pubkey: &str,
) {
    tokio::spawn(update_orders_forever(
        store.clone(),
        clt.clone(),
        market.to_string(),
        pubkey.to_string(),
    ));
    tokio::spawn(update_market_data_forever(
        store.clone(),
        clt.clone(),
        market.to_string(),
    ));
    tokio::spawn(update_position_forever(
        store.clone(),
        clt.clone(),
        market.to_string(),
        pubkey.to_string(),
    ));
    tokio::spawn(update_accounts_forever(
        store.clone(),
        clt.clone(),
        pubkey.to_string(),
    ));
}

async fn update_accounts_forever(
    store: Arc<Mutex<VegaStore>>,
    mut clt: TradingDataServiceClient<tonic::transport::Channel>,
    pubkey: String,
) {
    use vega_protobufs::datanode::api::v2::observe_accounts_response::Response;

    info!("starting accounts stream...");
    let mut stream = match clt
        .observe_accounts(ObserveAccountsRequest {
            party_id: pubkey,
            ..Default::default()
        })
        .await
    {
        Ok(s) => s.into_inner(),
        Err(e) => panic!("{:?}", e),
    };

    while let Some(item) = stream.next().await {
        match item {
            Ok(resp) => match resp.response {
                Some(r) => match r {
                    Response::Snapshot(o) => {
                        store.lock().unwrap().save_accounts(o.accounts.clone())
                    }
                    Response::Updates(o) => store.lock().unwrap().save_accounts(o.accounts.clone()),
                },
                _ => {}
            },
            _ => {}
        }
    }
}

async fn update_orders_forever(
    store: Arc<Mutex<VegaStore>>,
    mut clt: TradingDataServiceClient<tonic::transport::Channel>,
    market: String,
    pubkey: String,
) {
    use vega_protobufs::datanode::api::v2::observe_orders_response::Response;

    info!("starting orders stream...");
    let mut stream = match clt
        .observe_orders(ObserveOrdersRequest {
            party_id: Some(pubkey),
            market_id: Some(market),
            exclude_liquidity: Some(false),
        })
        .await
    {
        Ok(s) => s.into_inner(),
        Err(e) => panic!("{:?}", e),
    };

    while let Some(item) = stream.next().await {
        match item {
            Ok(resp) => match resp.response {
                Some(r) => match r {
                    Response::Snapshot(o) => store.lock().unwrap().save_orders(o.orders.clone()),
                    Response::Updates(o) => store.lock().unwrap().save_orders(o.orders.clone()),
                },
                _ => {}
            },
            _ => {}
        }
    }
}

async fn update_position_forever(
    store: Arc<Mutex<VegaStore>>,
    mut clt: TradingDataServiceClient<tonic::transport::Channel>,
    market: String,
    pubkey: String,
) {
    use vega_protobufs::datanode::api::v2::observe_positions_response::Response;
    info!("starting positions stream...");
    let mut stream = match clt
        .observe_positions(ObservePositionsRequest {
            party_id: Some(pubkey),
            market_id: Some(market),
        })
        .await
    {
        Ok(s) => s.into_inner(),
        Err(e) => panic!("{:?}", e),
    };

    while let Some(item) = stream.next().await {
        match item {
            Ok(resp) => match resp.response {
                Some(r) => match r {
                    Response::Snapshot(o) => {
                        store.lock().unwrap().save_positions(o.positions.clone())
                    }
                    Response::Updates(o) => {
                        store.lock().unwrap().save_positions(o.positions.clone())
                    }
                },
                _ => {}
            },
            _ => {}
        }
    }
}

async fn update_market_data_forever(
    store: Arc<Mutex<VegaStore>>,
    mut clt: TradingDataServiceClient<tonic::transport::Channel>,
    market: String,
) {
    info!("starting market data stream...");
    let mut stream = match clt
        .observe_markets_data(ObserveMarketsDataRequest {
            market_ids: vec![market],
        })
        .await
    {
        Ok(s) => s.into_inner(),
        Err(e) => panic!("{:?}", e),
    };

    while let Some(item) = stream.next().await {
        for md in item.unwrap().market_data.iter() {
            info!("received market data: {:?}", md);
            store.lock().unwrap().save_market_data(md.clone())
        }
    }
}

#[derive(Debug)]
pub enum Error {
    GrpcTransportError(tonic::transport::Error),
    GrpcError(tonic::Status),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "wallet client error: {}", self.desc())
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(error: tonic::transport::Error) -> Self {
        Error::GrpcTransportError(error)
    }
}

impl From<tonic::Status> for Error {
    fn from(error: tonic::Status) -> Self {
        Error::GrpcError(error)
    }
}
impl StdError for Error {}

impl Error {
    pub fn desc(&self) -> String {
        use Error::*;
        match self {
            GrpcTransportError(e) => format!("GRPC transport error: {}", e),
            GrpcError(e) => format!("GRPC error: {}", e),
        }
    }
}
