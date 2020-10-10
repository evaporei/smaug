# smaug

:moneybag: Bank microservice using Rust + Actix-web + EDN + CruxDB/transistor

![conversations_with_smaug](https://user-images.githubusercontent.com/15306309/95655246-4a443900-0adc-11eb-8482-1fda6eda5cc6.jpg)

## Features

- Create an account (`POST /accounts`)
- Get an account (`GET /accounts/:id`)
- Deposit into an account (`POST /accounts/:id/deposit`)
- Withdraw from an account (`POST /accounts/:id/withdraw`)
- Transfer from an account to another (`POST /accounts/:source-id/transfer`)
- Get account history (`GET /accounts/:id/history`)
- Get account operations (`GET /accounts/:id/operations`)

The code still needs improvement since its basically just one file now, so here's the list of things missing here:

- [ ] Modularize code
- [ ] Add request input validation (400)
- [ ] Add automated integration tests
