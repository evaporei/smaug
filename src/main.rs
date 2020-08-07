use actix_web::{web::{self, Bytes}, middleware::{DefaultHeaders}, App, HttpResponse, HttpServer};
use edn_rs::{parse_edn, Edn, Serialize, ser_struct};
use transistor::types::{CruxId, error::CruxError};
use transistor::http::Action;
use transistor::client::Crux;

use actix::prelude::*;

struct DbExecutor(Crux);

impl Actor for DbExecutor {
    type Context = SyncContext<Self>;
}

struct CreateAccount {
    account: DbAccount,
}

impl Message for CreateAccount {
    type Result = Result<DbAccount, CruxError>;
}

impl Handler<CreateAccount> for DbExecutor {
    type Result = Result<DbAccount, CruxError>;

    fn handle(&mut self, msg: CreateAccount, _: &mut Self::Context) -> Self::Result {
        // let uuid = format!("{}", uuid::Uuid::new_v4());
        let db_account = msg.account;

        let client = self.0.http_client();
        let action1 = Action::Put(db_account.clone().serialize());
        client.tx_log(vec![action1])?;

        Ok(db_account)
    }
}

ser_struct! {
#[allow(non_snake_case)]
#[derive(Clone, Debug)]
struct DbAccount {
    crux__db___id: CruxId,
    account___amount: usize,
}
}

impl From<Edn> for DbAccount {
    fn from(body: Edn) -> Self {
        Self {
            crux__db___id: CruxId::new("uuid"),
            account___amount: body[":amount"].to_uint().unwrap_or(0),
        }
    }
}

mod adapter {
    use super::*;
    pub(crate) fn account_edn_to_db(edn: Edn) -> DbAccount {
        edn.into()
    }
}

struct State {
    db: Addr<DbExecutor>,
}

async fn create_account(data: web::Data<State>, bytes: Bytes) -> Result<HttpResponse, HttpResponse> {
    let body = String::from_utf8(bytes.to_vec())
        .map_err(|_| HttpResponse::BadRequest().finish())?;
    let edn_body = parse_edn(&body)
        .map_err(|_| HttpResponse::BadRequest().finish())?;

    let db_account = adapter::account_edn_to_db(edn_body);

    let response = data.db.send(CreateAccount { account: db_account }).await;
    let account = response.map_err(|_| HttpResponse::InternalServerError().finish())?
        .map_err(|_| HttpResponse::InternalServerError().finish())?;

    Ok(HttpResponse::Created()
        .content_type("application/edn")
        .body(account.serialize()))
}

fn main() {
    let sys = actix::System::new("app");

    // Start 3 parallel db executors
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
    })
    .bind("127.0.0.1:8000")
    .unwrap()
    .run();

    println!("Started http server: 127.0.0.1:8080");
    let _ = sys.run();
}
