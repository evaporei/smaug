use actix_web::{web::{self, Bytes}, middleware::{DefaultHeaders}, App, HttpResponse, HttpServer};
use edn_rs::{parse_edn};

async fn create_account(bytes: Bytes) -> Result<HttpResponse, HttpResponse> {
    let body = String::from_utf8(bytes.to_vec())
        .map_err(|_| HttpResponse::BadRequest().finish())?;
    let edn_body = parse_edn(&body)
        .map_err(|_| HttpResponse::BadRequest().finish())?;
    println!("edn_body {}", edn_body);
    Ok(HttpResponse::Ok()
        .content_type("application/edn")
        .body("{:ok \"no error brah\"}".to_string()))
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .wrap(DefaultHeaders::new()
                .header("Content-Type", "application/edn")
                .header("Accept", "application/edn"))
            .route("/accounts", web::post().to(create_account))
    })
    .bind("127.0.0.1:8000")?
    .run()
    .await
}
