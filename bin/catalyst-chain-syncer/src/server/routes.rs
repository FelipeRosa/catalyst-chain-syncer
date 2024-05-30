use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_with::{hex::Hex, serde_as};
use tracing::error;

fn default_allow_multi_delegations() -> bool {
    true
}

#[serde_as]
#[derive(Deserialize)]
pub struct RegistrationsQueryParams {
    // TODO: Make this a path param?
    #[serde_as(as = "Option<Hex>")]
    pub stake_credential: Option<[u8; 28]>,
    pub max_slot: Option<u64>,
    #[serde(default = "default_allow_multi_delegations")]
    pub allow_multi_delegations: bool,
}

#[serde_as]
#[derive(Serialize)]
pub struct RegistrationsResponseEntry {
    #[serde_as(as = "Hex")]
    pub stake_credential: [u8; 28],
    pub voting_power: u64,
}

pub async fn get_registrations(
    State(conn_pool): State<Arc<postgres_store::ConnectionPool>>,
    Query(params): Query<RegistrationsQueryParams>,
) -> impl IntoResponse {
    let conn = match conn_pool.get().await {
        Ok(conn) => conn,
        Err(e) => {
            error!(error = ?e, "Failed to get connection from pool");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(()).into_response());
        }
    };

    let result = conn
        .client()
        .query(
            include_str!("../../server_sql/get_registrations.sql"),
            &[
                &(params.max_slot.map(|v| v as i64)),
                &params.stake_credential.as_ref().map(|v| v.as_slice()),
                &params.allow_multi_delegations,
            ],
        )
        .await;

    let rows = match result {
        Ok(rows) => rows,
        Err(e) => {
            error!(error = ?e, "Failed to get catalyst registrations");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(()).into_response());
        }
    };

    if rows.is_empty() {
        return (StatusCode::NOT_FOUND, Json(()).into_response());
    }

    let mut registrations = Vec::new();

    for row in rows {
        let mut stake_credential = [0u8; 28];
        stake_credential.copy_from_slice(row.get::<_, &[u8]>(0));

        let voting_power = row.get::<_, i64>(1) as u64;

        registrations.push(RegistrationsResponseEntry {
            stake_credential,
            voting_power,
        });
    }

    (StatusCode::OK, Json(registrations).into_response())
}
