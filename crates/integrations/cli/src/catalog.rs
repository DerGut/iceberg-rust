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

use std::any::Any;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use fs_err::read_to_string;
use futures::future::try_join_all;
use iceberg::Catalog;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_datafusion::IcebergCatalogProvider;
use toml::{Table as TomlTable, Value};

const CONFIG_NAME_CATALOGS: &str = "catalogs";

#[derive(Debug)]
pub struct IcebergCatalogList {
    catalogs: HashMap<String, Arc<dyn Catalog>>,
}

impl IcebergCatalogList {
    pub fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        self.catalogs.get(name).map(|catalog| Arc::clone(catalog))
    }

    pub async fn try_to_provider_list(&self) -> anyhow::Result<Arc<dyn CatalogProviderList>> {
        let provider_futures = self.catalogs.iter().map(|(name, catalog)| async move {
            let provider = IcebergCatalogProvider::try_new(Arc::clone(&catalog)).await?;
            Ok::<(String, Arc<dyn CatalogProvider>), anyhow::Error>((
                name.clone(),
                Arc::new(provider) as Arc<dyn CatalogProvider>,
            ))
        });
        let providers = try_join_all(provider_futures).await?;

        Ok(Arc::new(IcebergCatalogProviderList {
            providers: HashMap::from_iter(providers),
        }) as Arc<dyn CatalogProviderList>)
    }

    pub async fn parse(path: &Path) -> anyhow::Result<Self> {
        let toml_table: TomlTable = toml::from_str(&read_to_string(path)?)?;
        Self::try_from_toml(&toml_table).await
    }

    async fn try_from_toml(configs: &TomlTable) -> anyhow::Result<Self> {
        if let Value::Array(catalogs_config) =
            configs.get(CONFIG_NAME_CATALOGS).ok_or_else(|| {
                anyhow::Error::msg(format!("{CONFIG_NAME_CATALOGS} entry not found in config"))
            })?
        {
            let mut catalogs = HashMap::with_capacity(catalogs_config.len());
            for config in catalogs_config {
                if let Value::Table(table_config) = config {
                    let (name, catalog) = Self::parse_catalog(table_config)?;
                    catalogs.insert(name, catalog);
                } else {
                    return Err(anyhow!("{CONFIG_NAME_CATALOGS} entry must be a table"));
                }
            }
            Ok(Self { catalogs })
        } else {
            Err(anyhow!("{CONFIG_NAME_CATALOGS} must be an array of table!"))
        }
    }

    fn parse_catalog(config: &TomlTable) -> anyhow::Result<(String, Arc<dyn Catalog>)> {
        let name = config
            .get("name")
            .ok_or_else(|| anyhow::anyhow!("name not found for catalog"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("name is not string"))?;

        let r#type = config
            .get("type")
            .ok_or_else(|| anyhow::anyhow!("type not found for catalog"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("type is not string"))?;

        if r#type != "rest" {
            return Err(anyhow::anyhow!("Only rest catalog is supported for now!"));
        }

        let catalog_config = config
            .get("config")
            .ok_or_else(|| anyhow::anyhow!("config not found for catalog {name}"))?
            .as_table()
            .ok_or_else(|| anyhow::anyhow!("config is not table for catalog {name}"))?;

        let uri = catalog_config
            .get("uri")
            .ok_or_else(|| anyhow::anyhow!("uri not found for catalog {name}"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("uri is not string"))?
            .trim_end_matches('/');

        let warehouse = catalog_config
            .get("warehouse")
            .ok_or_else(|| anyhow::anyhow!("warehouse not found for catalog {name}"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("warehouse is not string for catalog {name}"))?;

        let props_table = catalog_config
            .get("props")
            .ok_or_else(|| anyhow::anyhow!("props not found for catalog {name}"))?
            .as_table()
            .ok_or_else(|| anyhow::anyhow!("props is not table for catalog {name}"))?;

        let mut props = HashMap::with_capacity(props_table.len());
        for (key, value) in props_table {
            let value_str = value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("props {key} is not string"))?;
            props.insert(key.to_string(), value_str.to_string());
        }

        let rest_catalog_config = RestCatalogConfig::builder()
            .uri(uri.to_string())
            .warehouse(warehouse.to_string())
            .props(props)
            .build();

        let catalog = RestCatalog::new(rest_catalog_config);

        Ok((name.to_string(), Arc::new(catalog) as Arc<dyn Catalog>))
    }
}

#[derive(Debug)]
struct IcebergCatalogProviderList {
    providers: HashMap<String, Arc<dyn CatalogProvider>>,
}

impl CatalogProviderList for IcebergCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn datafusion::catalog::CatalogProvider>,
    ) -> Option<Arc<dyn datafusion::catalog::CatalogProvider>> {
        tracing::error!("Registering catalog is not supported yet");
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        self.providers.keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.providers
            .get(name)
            .map(|provider| Arc::clone(provider))
    }
}
