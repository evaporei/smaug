use actix_web::{middleware::DefaultHeaders, web, App, HttpResponse, HttpServer};
use chrono::{DateTime, FixedOffset, Utc};
use edn_derive::{Deserialize, Serialize};
use edn_rs::{Edn, EdnError};
use std::str::FromStr;
use transistor::client::Crux;
use transistor::edn_rs;
use transistor::types::http::{Action, Order};
use transistor::types::{error::CruxError, query::Query, response::EntityHistoryElement, CruxId};
use uuid::Uuid;

use actix::prelude::*;

struct DbExecutor(Crux);

impl Actor for DbExecutor {
    type Context = SyncContext<Self>;
}

#[derive(Debug)]
enum DbError {
    NilEntity,
    StateConflict,
    CruxError(CruxError),
    EdnError(EdnError),
}

impl From<CruxError> for DbError {
    fn from(crux_error: CruxError) -> Self {
        DbError::CruxError(crux_error)
    }
}

impl From<EdnError> for DbError {
    fn from(edn_error: EdnError) -> Self {
        DbError::EdnError(edn_error)
    }
}

struct CreateAccount {
    account: DbAccount,
}

impl Message for CreateAccount {
    type Result = Result<DbAccount, DbError>;
}

impl Handler<CreateAccount> for DbExecutor {
    type Result = Result<DbAccount, DbError>;

    fn handle(&mut self, msg: CreateAccount, _: &mut Self::Context) -> Self::Result {
        let db_account = msg.account;

        let client = self.0.http_client();
        let action1 = Action::Put(edn_rs::to_string(db_account.clone()), None);

        let tx_time = Utc::now().to_string();
        let account_operation = DbAccountOperation {
            crux__db___id: CruxId::new(&Uuid::new_v4().to_string()),
            account_operation___type: OperationType::Create,
            account_operation___amount: db_account.account___amount,
            account_operation___source_account_id: db_account.crux__db___id.clone(),
            account_operation___target_account_id: None,
            tx___tx_time: Some(tx_time.clone()),
        };
        let action2 = Action::Put(
            edn_rs::to_string(account_operation),
            Some(tx_time.parse::<DateTime<FixedOffset>>().unwrap()),
        );

        client.tx_log(vec![action1, action2])?;

        Ok(db_account)
    }
}

struct GetAccount {
    account_id: String,
}

impl Message for GetAccount {
    type Result = Result<DbAccount, DbError>;
}

impl Handler<GetAccount> for DbExecutor {
    type Result = Result<DbAccount, DbError>;

    fn handle(&mut self, msg: GetAccount, _: &mut Self::Context) -> Self::Result {
        let client = self.0.http_client();
        let crux_account = client.entity(edn_rs::to_string(CruxId::new(&msg.account_id)))?;

        if crux_account == Edn::Nil {
            return Err(DbError::NilEntity);
        }

        Ok(edn_rs::from_edn(&crux_account)?)
    }
}

struct AccountDeposit {
    account_id: String,
    amount: usize,
}

impl Message for AccountDeposit {
    type Result = Result<DbAccount, DbError>;
}

impl Handler<AccountDeposit> for DbExecutor {
    type Result = Result<DbAccount, DbError>;

    fn handle(&mut self, msg: AccountDeposit, _: &mut Self::Context) -> Self::Result {
        let client = self.0.http_client();
        let crux_account = client.entity(edn_rs::to_string(CruxId::new(&msg.account_id)))?;

        if crux_account == Edn::Nil {
            return Err(DbError::NilEntity);
        }

        let mut db_account: DbAccount = edn_rs::from_edn(&crux_account)?;

        db_account.account___amount += msg.amount;

        let action1 = Action::Put(edn_rs::to_string(db_account.clone()), None);

        let tx_time = Utc::now().to_string();
        let account_operation = DbAccountOperation {
            crux__db___id: CruxId::new(&Uuid::new_v4().to_string()),
            account_operation___type: OperationType::Deposit,
            account_operation___amount: msg.amount,
            account_operation___source_account_id: db_account.crux__db___id.clone(),
            account_operation___target_account_id: None,
            tx___tx_time: Some(tx_time.clone()),
        };
        let action2 = Action::Put(
            edn_rs::to_string(account_operation),
            Some(tx_time.parse::<DateTime<FixedOffset>>().unwrap()),
        );

        client.tx_log(vec![action1, action2])?;

        Ok(db_account)
    }
}

struct AccountWithdraw {
    account_id: String,
    amount: usize,
}

impl Message for AccountWithdraw {
    type Result = Result<DbAccount, DbError>;
}

impl Handler<AccountWithdraw> for DbExecutor {
    type Result = Result<DbAccount, DbError>;

    fn handle(&mut self, msg: AccountWithdraw, _: &mut Self::Context) -> Self::Result {
        let client = self.0.http_client();
        let crux_account = client.entity(edn_rs::to_string(CruxId::new(&msg.account_id)))?;

        if crux_account == Edn::Nil {
            return Err(DbError::NilEntity);
        }

        let mut db_account: DbAccount = edn_rs::from_edn(&crux_account)?;

        db_account.account___amount -= msg.amount;

        let action1 = Action::Put(edn_rs::to_string(db_account.clone()), None);

        let tx_time = Utc::now().to_string();
        let account_operation = DbAccountOperation {
            crux__db___id: CruxId::new(&Uuid::new_v4().to_string()),
            account_operation___type: OperationType::Withdraw,
            account_operation___amount: msg.amount,
            account_operation___source_account_id: db_account.crux__db___id.clone(),
            account_operation___target_account_id: None,
            tx___tx_time: Some(tx_time.clone()),
        };
        let action2 = Action::Put(
            edn_rs::to_string(account_operation),
            Some(tx_time.parse::<DateTime<FixedOffset>>().unwrap()),
        );
        client.tx_log(vec![action1, action2])?;

        Ok(db_account)
    }
}

struct AccountTransfer {
    source_account_id: String,
    amount: usize,
    target_account_id: String,
}

impl Message for AccountTransfer {
    type Result = Result<DbAccount, DbError>;
}

impl Handler<AccountTransfer> for DbExecutor {
    type Result = Result<DbAccount, DbError>;

    fn handle(&mut self, msg: AccountTransfer, _: &mut Self::Context) -> Self::Result {
        let client = self.0.http_client();
        let crux_source_account =
            client.entity(edn_rs::to_string(CruxId::new(&msg.source_account_id)))?;

        if crux_source_account == Edn::Nil {
            return Err(DbError::NilEntity);
        }

        let mut db_source_account: DbAccount = edn_rs::from_edn(&crux_source_account)?;

        if db_source_account.account___amount < msg.amount {
            return Err(DbError::StateConflict);
        }

        let crux_target_account =
            client.entity(edn_rs::to_string(CruxId::new(&msg.target_account_id)))?;

        if crux_target_account == Edn::Nil {
            return Err(DbError::NilEntity);
        }

        let mut db_target_account: DbAccount = edn_rs::from_edn(&crux_target_account)?;

        db_source_account.account___amount -= msg.amount;
        db_target_account.account___amount += msg.amount;

        let action1 = Action::Put(edn_rs::to_string(db_source_account.clone()), None);
        let action2 = Action::Put(edn_rs::to_string(db_target_account.clone()), None);

        let tx_time = Utc::now().to_string();
        let account_operation = DbAccountOperation {
            crux__db___id: CruxId::new(&Uuid::new_v4().to_string()),
            account_operation___type: OperationType::Transfer,
            account_operation___amount: msg.amount,
            account_operation___source_account_id: db_source_account.crux__db___id.clone(),
            account_operation___target_account_id: Some(db_target_account.crux__db___id.clone()),
            tx___tx_time: Some(tx_time.clone()),
        };
        let action3 = Action::Put(
            edn_rs::to_string(account_operation),
            Some(tx_time.parse::<DateTime<FixedOffset>>().unwrap()),
        );
        client.tx_log(vec![action1, action2, action3])?;

        Ok(db_source_account)
    }
}

struct AccountHistory {
    account_id: String,
}

impl Message for AccountHistory {
    type Result = Result<Vec<ResponseAccountHistoryElement>, DbError>;
}

impl Handler<AccountHistory> for DbExecutor {
    type Result = Result<Vec<ResponseAccountHistoryElement>, DbError>;

    fn handle(&mut self, msg: AccountHistory, _: &mut Self::Context) -> Self::Result {
        let client = self.0.http_client();
        let response = client.entity_history(
            edn_rs::to_string(CruxId::new(&msg.account_id)),
            Order::Desc,
            true,
        )?;

        if response.history.is_empty() {
            return Err(DbError::NilEntity);
        }

        Ok(response
            .history
            .into_iter()
            .map(ResponseAccountHistoryElement::from)
            .collect::<Vec<ResponseAccountHistoryElement>>())
    }
}

struct AccountOperations {
    account_id: String,
}

impl Message for AccountOperations {
    type Result = Result<Vec<DbAccountOperation>, DbError>;
}

impl Handler<AccountOperations> for DbExecutor {
    type Result = Result<Vec<DbAccountOperation>, DbError>;

    fn handle(&mut self, msg: AccountOperations, _: &mut Self::Context) -> Self::Result {
        let client = self.0.http_client();
        let response = client.entity(edn_rs::to_string(CruxId::new(&msg.account_id)))?;

        if response == Edn::Nil {
            return Err(DbError::NilEntity);
        }

        let query = Query::find(vec![
            "?account-operation",
            // "?tx-time",
        ])?
        .where_clause(vec![
            "?account-operation :account-operation/source-account-id ?account-id",
            // "?account-operation :tx/tx-time ?tx-time",
        ])?
        // ORDER TIME IDIOT
        // .order_by(vec!["?tx-time :asc"])?
        .args(vec![&format!("?account-id :{}", msg.account_id)])?
        .build()?;

        let operations = client.query(query)?;

        Ok(operations
            .iter()
            .map(|a| {
                let id = edn_rs::to_string(CruxId::new(&a[0]));

                let edn_body = client.entity(id).unwrap();
                edn_body
            })
            .map(|edn| edn_rs::from_edn(&edn))
            .collect::<Result<Vec<DbAccountOperation>, EdnError>>()?)
    }
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Clone, Debug)]
struct DbAccount {
    crux__db___id: CruxId,   // :crux.db/id
    account___amount: usize, // :account/amount
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum OperationType {
    Create,
    Deposit,
    Withdraw,
    Transfer,
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Clone, Debug)]
struct DbAccountOperation {
    crux__db___id: CruxId,                                 // :crux.db/id
    account_operation___type: OperationType,               // :account-operation/type
    account_operation___amount: usize,                     // :account-operation/amount
    account_operation___source_account_id: CruxId,         // :account-operation/source-account-id
    account_operation___target_account_id: Option<CruxId>, // :account-operation/target-account-id
    tx___tx_time: Option<String>,                          // :tx/tx-time
}

struct State {
    db: Addr<DbExecutor>,
}

#[derive(Serialize)]
struct ResponseAccount {
    id: String,
    amount: usize,
}

impl From<DbAccount> for ResponseAccount {
    fn from(db_account: DbAccount) -> Self {
        let mut uuid_without_colon = edn_rs::to_string(db_account.crux__db___id);
        uuid_without_colon.remove(0);

        Self {
            id: uuid_without_colon,
            amount: db_account.account___amount,
        }
    }
}

#[derive(Serialize)]
struct ResponseAccountHistoryElement {
    id: String,
    amount: usize,
    time: String,
}

impl From<EntityHistoryElement> for ResponseAccountHistoryElement {
    fn from(history_element: EntityHistoryElement) -> Self {
        let edn_document = history_element.db__doc.unwrap();

        Self {
            id: edn_document[":crux.db/id"].to_string(),
            amount: edn_document[":account/amount"].to_uint().unwrap_or(0),
            time: history_element.tx___tx_time.to_string(),
        }
    }
}

#[derive(Serialize)]
struct ResponseAccountOperation {
    id: String,
    operation_type: OperationType,
    amount: usize,
    source_account_id: String,
    target_account_id: String,
    time: String,
}

impl From<DbAccountOperation> for ResponseAccountOperation {
    fn from(db_account_operation: DbAccountOperation) -> Self {
        let mut id_without_colon = edn_rs::to_string(db_account_operation.crux__db___id);
        id_without_colon.remove(0);

        let mut source_id_without_colon =
            edn_rs::to_string(db_account_operation.account_operation___source_account_id);
        source_id_without_colon.remove(0);

        let mut target_id_without_colon =
            edn_rs::to_string(db_account_operation.account_operation___target_account_id);
        target_id_without_colon.remove(0);

        Self {
            id: id_without_colon,
            operation_type: db_account_operation.account_operation___type,
            amount: db_account_operation.account_operation___amount,
            source_account_id: source_id_without_colon,
            target_account_id: target_id_without_colon,
            time: db_account_operation.tx___tx_time.unwrap(),
        }
    }
}

#[derive(Deserialize)]
struct RequestAccount {
    amount: usize,
}

impl From<RequestAccount> for DbAccount {
    fn from(req_account: RequestAccount) -> Self {
        Self {
            crux__db___id: CruxId::new(&Uuid::new_v4().to_string()),
            account___amount: req_account.amount,
        }
    }
}

async fn create_account(
    data: web::Data<State>,
    body: String,
) -> Result<HttpResponse, HttpResponse> {
    let req_account: RequestAccount =
        edn_rs::from_str(&body).map_err(|_| HttpResponse::BadRequest().finish())?;

    let response = data
        .db
        .send(CreateAccount {
            account: req_account.into(),
        })
        .await;
    let db_account = response
        .map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|_| HttpResponse::InternalServerError().finish())?;

    Ok(HttpResponse::Created()
        .content_type("application/edn")
        .body(edn_rs::to_string(ResponseAccount::from(db_account))))
}

async fn get_account(
    data: web::Data<State>,
    account_id: web::Path<String>,
) -> Result<HttpResponse, HttpResponse> {
    let response = data
        .db
        .send(GetAccount {
            account_id: account_id.to_string(),
        })
        .await;
    let db_account = response
        .map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|db_error| match db_error {
            DbError::NilEntity => HttpResponse::NotFound().finish(),
            _ => HttpResponse::InternalServerError().finish(),
        })?;

    Ok(HttpResponse::Ok()
        .content_type("application/edn")
        .body(edn_rs::to_string(ResponseAccount::from(db_account))))
}

async fn account_deposit(
    data: web::Data<State>,
    account_id: web::Path<String>,
    body: String,
) -> Result<HttpResponse, HttpResponse> {
    let edn_body = Edn::from_str(&body).map_err(|_| HttpResponse::BadRequest().finish())?;

    let account_id = account_id.to_string();
    let amount = edn_body[":amount"].to_uint().unwrap_or(0);

    let response = data.db.send(AccountDeposit { account_id, amount }).await;
    let db_account = response
        .map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|db_error| match db_error {
            DbError::NilEntity => HttpResponse::NotFound().finish(),
            _ => HttpResponse::InternalServerError().finish(),
        })?;

    Ok(HttpResponse::Ok()
        .content_type("application/edn")
        .body(edn_rs::to_string(ResponseAccount::from(db_account))))
}

async fn account_withdraw(
    data: web::Data<State>,
    account_id: web::Path<String>,
    body: String,
) -> Result<HttpResponse, HttpResponse> {
    let edn_body = Edn::from_str(&body).map_err(|_| HttpResponse::BadRequest().finish())?;

    let account_id = account_id.to_string();
    let amount = edn_body[":amount"].to_uint().unwrap_or(0);

    let response = data.db.send(AccountWithdraw { account_id, amount }).await;
    let db_account = response
        .map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|db_error| match db_error {
            DbError::NilEntity => HttpResponse::NotFound().finish(),
            _ => HttpResponse::InternalServerError().finish(),
        })?;

    Ok(HttpResponse::Ok()
        .content_type("application/edn")
        .body(edn_rs::to_string(ResponseAccount::from(db_account))))
}

async fn account_transfer(
    data: web::Data<State>,
    source_account_id: web::Path<String>,
    body: String,
) -> Result<HttpResponse, HttpResponse> {
    let edn_body = Edn::from_str(&body).map_err(|_| HttpResponse::BadRequest().finish())?;

    let source_account_id = source_account_id.to_string();
    let amount = edn_body[":amount"].to_uint().unwrap_or(0);
    let target_account_id: String = edn_rs::from_edn(&edn_body[":target-account-id"])
        .map_err(|_| HttpResponse::BadRequest().finish())?;

    let response = data
        .db
        .send(AccountTransfer {
            source_account_id,
            amount,
            target_account_id,
        })
        .await;
    let db_account = response
        .map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|db_error| match db_error {
            DbError::NilEntity => HttpResponse::NotFound().finish(),
            DbError::StateConflict => HttpResponse::Conflict().finish(),
            DbError::CruxError(_) => HttpResponse::InternalServerError().finish(),
            DbError::EdnError(_) => HttpResponse::InternalServerError().finish(),
        })?;

    Ok(HttpResponse::Ok()
        .content_type("application/edn")
        .body(edn_rs::to_string(ResponseAccount::from(db_account))))
}

async fn account_history(
    data: web::Data<State>,
    account_id: web::Path<String>,
) -> Result<HttpResponse, HttpResponse> {
    let response = data
        .db
        .send(AccountHistory {
            account_id: account_id.to_string(),
        })
        .await;
    let response_history = response
        .map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|db_error| match db_error {
            DbError::NilEntity => HttpResponse::NotFound().finish(),
            _ => HttpResponse::InternalServerError().finish(),
        })?;

    Ok(HttpResponse::Ok()
        .content_type("application/edn")
        .body(edn_rs::to_string(response_history)))
}

async fn account_operations(
    data: web::Data<State>,
    account_id: web::Path<String>,
) -> Result<HttpResponse, HttpResponse> {
    let response = data
        .db
        .send(AccountOperations {
            account_id: account_id.to_string(),
        })
        .await;
    let db_account_operations = response
        .map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|db_error| match db_error {
            DbError::NilEntity => HttpResponse::NotFound().finish(),
            _ => HttpResponse::InternalServerError().finish(),
        })?;

    let response_operations = db_account_operations
        .into_iter()
        .map(ResponseAccountOperation::from)
        .collect::<Vec<ResponseAccountOperation>>();

    Ok(HttpResponse::Ok()
        .content_type("application/edn")
        .body(edn_rs::to_string(response_operations)))
}

fn main() {
    let sys = actix::System::new("app");

    let addr = SyncArbiter::start(3, || DbExecutor(Crux::new("localhost", "3000")));

    HttpServer::new(move || {
        App::new()
            .data(State { db: addr.clone() })
            .wrap(
                DefaultHeaders::new()
                    .header("Content-Type", "application/edn")
                    .header("Accept", "application/edn"),
            )
            .route("/accounts", web::post().to(create_account))
            .route("/accounts/{account_id}", web::get().to(get_account))
            .route(
                "/accounts/{account_id}/deposit",
                web::post().to(account_deposit),
            )
            .route(
                "/accounts/{account_id}/withdraw",
                web::post().to(account_withdraw),
            )
            .route(
                "/accounts/{account_id}/transfer",
                web::post().to(account_transfer),
            )
            .route(
                "/accounts/{account_id}/history",
                web::get().to(account_history),
            )
            .route(
                "/accounts/{account_id}/operations",
                web::get().to(account_operations),
            )
    })
    .bind("127.0.0.1:8000")
    .unwrap()
    .run();

    println!("Started HTTP server: 127.0.0.1:8080");
    let _ = sys.run();
}
