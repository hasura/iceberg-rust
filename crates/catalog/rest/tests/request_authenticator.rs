// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind, Result};
use iceberg_catalog_rest::{RequestAuthenticator, RestCatalogBuilder};

#[derive(Debug, Default)]
struct RotatingAuth {
    calls: AtomicUsize,
    fail: AtomicBool,
}

#[async_trait::async_trait]
impl RequestAuthenticator for RotatingAuth {
    async fn authenticate(&self, request: &mut reqwest::Request) -> Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Unexpected, "No credential available"));
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        request.headers_mut().insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer token-{call}").parse().unwrap(),
        );
        Ok(())
    }
}

#[tokio::test]
async fn authenticates_config_and_every_catalog_request_after_extra_headers() {
    let mut server = mockito::Server::new_async().await;
    let config = server.mock("GET", "/v1/config")
        .match_header("authorization", "Bearer token-0")
        .with_status(200)
        .with_body(r#"{"defaults":{},"overrides":{"header.Authorization":"Bearer server-header","token":"server-token"}}"#)
        .expect(1).create_async().await;
    let first = server
        .mock("GET", "/v1/namespaces")
        .match_header("authorization", "Bearer token-1")
        .with_status(200)
        .with_body(r#"{"namespaces":[]}"#)
        .expect(1)
        .create_async()
        .await;
    let second = server
        .mock("GET", "/v1/namespaces")
        .match_header("authorization", "Bearer token-2")
        .with_status(200)
        .with_body(r#"{"namespaces":[]}"#)
        .expect(1)
        .create_async()
        .await;
    let auth = Arc::new(RotatingAuth::default());
    let catalog = RestCatalogBuilder::default()
        .with_authenticator(auth.clone())
        .load(
            "test",
            HashMap::from([
                ("uri".to_string(), server.url()),
                ("token".to_string(), "static-token".to_string()),
                (
                    "header.Authorization".to_string(),
                    "Bearer static-header".to_string(),
                ),
            ]),
        )
        .await
        .unwrap();
    catalog.list_namespaces(None).await.unwrap();
    catalog.list_namespaces(None).await.unwrap();
    assert_eq!(auth.calls.load(Ordering::SeqCst), 3);
    config.assert_async().await;
    first.assert_async().await;
    second.assert_async().await;

    auth.fail.store(true, Ordering::SeqCst);
    let unexpected = server
        .mock("GET", "/v1/namespaces")
        .expect(0)
        .create_async()
        .await;
    assert!(catalog.list_namespaces(None).await.is_err());
    unexpected.assert_async().await;
}

#[tokio::test]
async fn authentication_failure_prevents_initial_config_request() {
    let mut server = mockito::Server::new_async().await;
    let unexpected = server
        .mock("GET", "/v1/config")
        .expect(0)
        .create_async()
        .await;
    let auth = Arc::new(RotatingAuth::default());
    auth.fail.store(true, Ordering::SeqCst);
    let catalog = RestCatalogBuilder::default()
        .with_authenticator(auth)
        .load("test", HashMap::from([("uri".to_string(), server.url())]))
        .await
        .unwrap();
    assert!(catalog.list_namespaces(None).await.is_err());
    unexpected.assert_async().await;
}
