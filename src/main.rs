use actix_web::{web, middleware::{DefaultHeaders}, App, HttpResponse, HttpServer};
use edn_rs::{parse_edn, Edn, Serialize, ser_struct};
use transistor::types::{CruxId, error::CruxError};
use transistor::http::Action;
use transistor::client::Crux;
use uuid::Uuid;

use actix::prelude::*;

struct DbExecutor(Crux);

impl Actor for DbExecutor {
    type Context = SyncContext<Self>;
}

enum DbError {
    NilEntity,
    CruxError(CruxError),
}

impl From<CruxError> for DbError {
    fn from(crux_error: CruxError) -> Self {
        DbError::CruxError(crux_error)
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
        let action1 = Action::Put(db_account.clone().serialize());
        client.tx_log(vec![action1])?;

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
        let crux_account = client.entity(CruxId::new(&msg.account_id).serialize())?;

        if crux_account == Edn::Nil {
            return Err(DbError::NilEntity);
        }

        Ok(adapter::crux_account_edn_to_db_account(crux_account))
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
        let crux_account = client.entity(CruxId::new(&msg.account_id).serialize())?;

        if crux_account == Edn::Nil {
            return Err(DbError::NilEntity);
        }

        let mut db_account = adapter::crux_account_edn_to_db_account(crux_account);

        db_account.account___amount += msg.amount;

        let action1 = Action::Put(db_account.clone().serialize());
        client.tx_log(vec![action1])?;

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
        let crux_account = client.entity(CruxId::new(&msg.account_id).serialize())?;

        if crux_account == Edn::Nil {
            return Err(DbError::NilEntity);
        }

        let mut db_account = adapter::crux_account_edn_to_db_account(crux_account);

        db_account.account___amount -= msg.amount;

        let action1 = Action::Put(db_account.clone().serialize());
        client.tx_log(vec![action1])?;

        Ok(db_account)
    }
}

ser_struct! {
#[allow(non_snake_case)]
#[derive(Clone, Debug)]
struct DbAccount {
    crux__db___id: CruxId,  // :crux.db/id
    account___amount: usize,// :account/amount
}
}

impl From<AccountContainer> for DbAccount {
    fn from(account: AccountContainer) -> Self {
        match account {
            AccountContainer::Body(body) => Self {
                crux__db___id: CruxId::new(&Uuid::new_v4().to_string()),
                account___amount: body[":amount"].to_uint().unwrap_or(0),
            },
            AccountContainer::CruxEntity(edn) => Self {
                crux__db___id: CruxId::new(&edn[":crux.db/id"].to_string()),
                account___amount: edn[":account/amount"].to_uint().unwrap_or(0),
            }
        }
    }
}

enum AccountContainer {
    Body(Edn),
    CruxEntity(Edn),
}

mod adapter {
    use super::*;
    pub(crate) fn body_account_edn_to_db(edn: Edn) -> DbAccount {
        AccountContainer::Body(edn).into()
    }
    pub(crate) fn crux_account_edn_to_db_account(edn: Edn) -> DbAccount {
        AccountContainer::CruxEntity(edn).into()
    }
}

struct State {
    db: Addr<DbExecutor>,
}

ser_struct! {
struct ResponseAccount {
    id: String,
    amount: usize,
}
}

impl From<DbAccount> for ResponseAccount {
    fn from(db_account: DbAccount) -> Self {
        let mut uuid_without_colon = db_account.crux__db___id.serialize();
        uuid_without_colon.remove(0);

        Self {
            id: uuid_without_colon,
            amount: db_account.account___amount,
        }
    }
}

async fn create_account(data: web::Data<State>, body: String) -> Result<HttpResponse, HttpResponse> {
    let edn_body = parse_edn(&body)
        .map_err(|_| HttpResponse::BadRequest().finish())?;

    let db_account = adapter::body_account_edn_to_db(edn_body);

    let response = data.db.send(CreateAccount { account: db_account }).await;
    let db_account = response.map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|_| HttpResponse::InternalServerError().finish())?;

    Ok(HttpResponse::Created()
        .content_type("application/edn")
        .body(ResponseAccount::from(db_account).serialize()))
}

async fn get_account(data: web::Data<State>, account_id: web::Path<String>) -> Result<HttpResponse, HttpResponse> {
    let response = data.db.send(GetAccount { account_id: account_id.to_string() }).await;
    let db_account = response.map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|db_error| match db_error {
            DbError::NilEntity => HttpResponse::NotFound().finish(),
            DbError::CruxError(_) => HttpResponse::InternalServerError().finish(),
        })?;

    Ok(HttpResponse::Ok()
        .content_type("application/edn")
        .body(ResponseAccount::from(db_account).serialize()))
}

async fn account_deposit(data: web::Data<State>, account_id: web::Path<String>, body: String) -> Result<HttpResponse, HttpResponse> {
    let edn_body = parse_edn(&body)
        .map_err(|_| HttpResponse::BadRequest().finish())?;

    let account_id = account_id.to_string();
    let amount = edn_body[":amount"].to_uint().unwrap_or(0);

    let response = data.db.send(AccountDeposit { account_id, amount }).await;
    let db_account = response.map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|db_error| match db_error {
            DbError::NilEntity => HttpResponse::NotFound().finish(),
            DbError::CruxError(_) => HttpResponse::InternalServerError().finish(),
        })?;

    Ok(HttpResponse::Ok()
        .content_type("application/edn")
        .body(ResponseAccount::from(db_account).serialize()))
}

async fn account_withdraw(data: web::Data<State>, account_id: web::Path<String>, body: String) -> Result<HttpResponse, HttpResponse> {
    let edn_body = parse_edn(&body)
        .map_err(|_| HttpResponse::BadRequest().finish())?;

    let account_id = account_id.to_string();
    let amount = edn_body[":amount"].to_uint().unwrap_or(0);

    let response = data.db.send(AccountWithdraw { account_id, amount }).await;
    let db_account = response.map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|db_error| match db_error {
            DbError::NilEntity => HttpResponse::NotFound().finish(),
            DbError::CruxError(_) => HttpResponse::InternalServerError().finish(),
        })?;

    Ok(HttpResponse::Ok()
        .content_type("application/edn")
        .body(ResponseAccount::from(db_account).serialize()))
}

fn main() {
    let sys = actix::System::new("app");

    let addr = SyncArbiter::start(3, || {
        DbExecutor(Crux::new("localhost", "3000"))
    });

    HttpServer::new(move || {
        App::new()
            .data(State { db: addr.clone() })
            .wrap(DefaultHeaders::new()
                .header("Content-Type", "application/edn")
                .header("Accept", "application/edn"))
            .route("/accounts", web::post().to(create_account))
            .route("/accounts/{account_id}", web::get().to(get_account))
            .route("/accounts/{account_id}/deposit", web::post().to(account_deposit))
            .route("/accounts/{account_id}/withdraw", web::post().to(account_withdraw))
    })
    .bind("127.0.0.1:8000")
    .unwrap()
    .run();

    println!("Started HTTP server: 127.0.0.1:8080");
    let _ = sys.run();
}
