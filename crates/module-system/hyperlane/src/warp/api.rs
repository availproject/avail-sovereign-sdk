use axum::routing::get;
use serde::Serialize;
use sov_modules_api::prelude::axum::extract::State;
use sov_modules_api::prelude::utoipa::openapi::OpenApi;
use sov_modules_api::prelude::{axum, UnwrapInfallible};
use sov_modules_api::rest::utils::{errors, ApiResult, Path};
use sov_modules_api::rest::{ApiState, HasCustomRestApi};
use sov_modules_api::{ApiStateAccessor, HexHash, Spec};

use super::{RouterKey, WarpRouteId};
use crate::warp::Warp;

#[derive(Debug, Clone, Serialize)]
struct RemoteRouter {
    domain: u32,
    address: HexHash,
}

impl<S: Spec> HasCustomRestApi for Warp<S> {
    type Spec = S;

    fn custom_rest_api(&self, state: ApiState<S>) -> axum::Router<()> {
        axum::Router::new()
            .route("/route/:route/routers", get(Self::get_routers))
            .with_state(state.with(self.clone()))
    }

    fn custom_openapi_spec(&self) -> Option<OpenApi> {
        let mut open_api: OpenApi =
            serde_yaml::from_str(include_str!("openapi-v3.yaml")).expect("Invalid OpenAPI spec");
        // Because https://github.com/juhaku/utoipa/issues/972
        for path_item in open_api.paths.paths.values_mut() {
            path_item.extensions = None;
        }
        Some(open_api)
    }
}

impl<S: Spec> Warp<S> {
    async fn get_routers(
        State(state): State<ApiState<S, Self>>,
        mut accessor: ApiStateAccessor<S>,
        Path(route): Path<WarpRouteId>,
    ) -> ApiResult<Vec<RemoteRouter>> {
        let router = state
            .warp_routes
            .get(&route, &mut accessor)
            .unwrap_infallible()
            .ok_or(errors::not_found_404("warp route", route))?;

        let mut routers = Vec::with_capacity(router.enrolled_destinations.len());
        for domain in router.enrolled_destinations {
            let address = state
                .routers
                .get(
                    &RouterKey {
                        route_id: route,
                        remote_domain: domain,
                    },
                    &mut accessor,
                )
                .unwrap_infallible()
                .ok_or(errors::internal_server_error_response_500(format!(
                    "Domain {domain} was enrolled in route {route} but router for domain {domain} not found"
                )))?;
            routers.push(RemoteRouter {
                domain,
                address: address.0,
            });
        }
        Ok(routers.into())
    }
}
