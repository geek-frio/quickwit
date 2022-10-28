// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{bail, Context};
use byte_unit::Byte;
use json_comments::StripComments;
use quickwit_common::net::{find_private_ip, Host, HostAddr};
use quickwit_common::new_coolid;
use quickwit_common::uri::{Extension, Uri};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config_value::ConfigValue;
use crate::qw_env_vars::*;
use crate::service::QuickwitService;
use crate::templating::render_config;
use crate::validate_identifier;

pub const DEFAULT_QW_CONFIG_PATH: &str = "config/quickwit.yaml";

pub const DEFAULT_CLUSTER_ID: &str = "quickwit-default-cluster";

pub const DEFAULT_DATA_DIR_PATH: &str = "qwdata";

// Default config values in the order they appear in [`QuickwitConfigBuilder`].
fn default_cluster_id() -> ConfigValue<String, QW_CLUSTER_ID> {
    ConfigValue::new(DEFAULT_CLUSTER_ID.to_string())
}

fn default_node_id() -> ConfigValue<String, QW_NODE_ID> {
    ConfigValue::new(new_coolid("node"))
}

fn default_enabled_services() -> ConfigValue<List, QW_ENABLED_SERVICES> {
    ConfigValue::new(List(
        QuickwitService::supported_services()
            .into_iter()
            .map(|service| service.to_string())
            .collect(),
    ))
}

fn default_listen_address() -> ConfigValue<String, QW_LISTEN_ADDRESS> {
    ConfigValue::new(Host::default().to_string())
}

fn default_rest_listen_port() -> ConfigValue<u16, QW_REST_LISTEN_PORT> {
    ConfigValue::new(7280)
}

fn default_data_dir_uri() -> ConfigValue<Uri, QW_DATA_DIR> {
    ConfigValue::new(Uri::try_new(DEFAULT_DATA_DIR_PATH).)
}

// Surprisingly, the default metastore and the index root uri are the same (if you exclude the
// polling_interval parameter). Indeed, this is a convenient setting for testing with a file backed
// metastore and indexes splits stored locally too.
// For a given index `index-id`, it means that we have the metastore file
// in  `./qwdata/indexes/{index-id}/metastore.json` and splits in
// dir `./qwdata/indexes/{index-id}/splits`.
fn default_metastore_uri(data_dir_uri: &Uri) -> Uri {
    data_dir_uri.join("indexes#polling_interval=30s").expect("Failed to create default metastore URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
}

// See comment above.
fn default_index_root_uri(data_dir_uri: &Uri) -> Uri {
    data_dir_uri.join("indexes").expect("Failed to create default index root URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerConfig {
    #[doc(hidden)]
    #[serde(default = "IndexerConfig::default_enable_opentelemetry_otlp_service")]
    pub enable_opentelemetry_otlp_service: bool,
    #[serde(default = "IndexerConfig::default_split_store_max_num_bytes")]
    pub split_store_max_num_bytes: Byte,
    #[serde(default = "IndexerConfig::default_split_store_max_num_splits")]
    pub split_store_max_num_splits: usize,
    #[serde(default = "IndexerConfig::default_max_concurrent_split_uploads")]
    pub max_concurrent_split_uploads: usize,
}

impl IndexerConfig {
    fn default_enable_opentelemetry_otlp_service() -> bool {
        false
    }

    fn default_max_concurrent_split_uploads() -> usize {
        4
    }

    pub fn default_split_store_max_num_bytes() -> Byte {
        Byte::from_bytes(100_000_000_000) // 100G
    }

    pub fn default_split_store_max_num_splits() -> usize {
        1_000
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> anyhow::Result<Self> {
        let indexer_config = IndexerConfig {
            enable_opentelemetry_otlp_service: true,
            split_store_max_num_bytes: Byte::from_bytes(1_000_000),
            split_store_max_num_splits: 3,
            max_concurrent_split_uploads: 4,
        };
        Ok(indexer_config)
    }
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            enable_opentelemetry_otlp_service: Self::default_enable_opentelemetry_otlp_service(),
            split_store_max_num_bytes: Self::default_split_store_max_num_bytes(),
            split_store_max_num_splits: Self::default_split_store_max_num_splits(),
            max_concurrent_split_uploads: Self::default_max_concurrent_split_uploads(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearcherConfig {
    #[doc(hidden)]
    #[serde(default = "SearcherConfig::default_enable_jaeger_service")]
    pub enable_jaeger_service: bool,
    #[serde(default = "SearcherConfig::default_fast_field_cache_capacity")]
    pub fast_field_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_split_footer_cache_capacity")]
    pub split_footer_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_max_num_concurrent_split_searches")]
    pub max_num_concurrent_split_searches: usize,
    #[serde(default = "SearcherConfig::default_max_num_concurrent_split_streams")]
    pub max_num_concurrent_split_streams: usize,
}

impl SearcherConfig {
    fn default_enable_jaeger_service() -> bool {
        false
    }

    fn default_fast_field_cache_capacity() -> Byte {
        Byte::from_bytes(1_000_000_000) // 1G
    }

    fn default_split_footer_cache_capacity() -> Byte {
        Byte::from_bytes(500_000_000) // 500M
    }

    fn default_max_num_concurrent_split_searches() -> usize {
        100
    }

    fn default_max_num_concurrent_split_streams() -> usize {
        100
    }
}

impl Default for SearcherConfig {
    fn default() -> Self {
        Self {
            enable_jaeger_service: Self::default_enable_jaeger_service(),
            fast_field_cache_capacity: Self::default_fast_field_cache_capacity(),
            split_footer_cache_capacity: Self::default_split_footer_cache_capacity(),
            max_num_concurrent_split_streams: Self::default_max_num_concurrent_split_streams(),
            max_num_concurrent_split_searches: Self::default_max_num_concurrent_split_searches(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
struct List(Vec<String>);

impl FromStr for List {
    type Err = anyhow::Error;

    fn from_str(list_str: &str) -> Result<Self, Self::Err> {
        let list = list_str
            .split(',')
            .map(|elem| elem.trim().to_string())
            .filter(|elem| !elem.is_empty())
            .collect();
        Ok(List(list))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct QuickwitConfigBuilder {
    version: ConfigValue<usize, QW_NONE>,
    #[serde(default = "default_cluster_id")]
    cluster_id: ConfigValue<String, QW_CLUSTER_ID>,
    #[serde(default = "default_node_id")]
    node_id: ConfigValue<String, QW_NODE_ID>,
    #[serde(default = "default_enabled_services")]
    enabled_services: ConfigValue<List, QW_ENABLED_SERVICES>,
    #[serde(default = "default_listen_address")]
    listen_address: ConfigValue<String, QW_LISTEN_ADDRESS>,
    advertise_address: Option<String>,
    #[serde(default = "default_rest_listen_port")]
    rest_listen_port: ConfigValue<u16, QW_REST_LISTEN_PORT>,
    gossip_listen_port: Option<ConfigValue<u16, QW_GOSSIP_LISTEN_PORT>>,
    grpc_listen_port: Option<ConfigValue<u16, QW_GRPC_LISTEN_PORT>>,
    #[serde(default)]
    peer_seeds: ConfigValue<List, QW_PEER_SEEDS>,
    #[serde(default = "default_data_dir_path")]
    #[serde(rename = "data_dir")]
    data_dir_uri: ConfigValue<Uri, QW_DATA_DIR>,
    #[serde(default)]
    metastore_uri: ConfigValue<Option<Uri>, QW_METASTORE_URI>,
    #[serde(default)]
    default_index_root_uri: ConfigValue<Option<Uri>, QW_DEFAULT_ROOT_INDEX_URI>,
    #[serde(rename = "indexer")]
    #[serde(default)]
    indexer_config: IndexerConfig,
    #[serde(rename = "searcher")]
    #[serde(default)]
    searcher_config: SearcherConfig,
}

impl QuickwitConfigBuilder {
    async fn from_uri(uri: &Uri, config_content: &[u8]) -> anyhow::Result<Self> {
        let parser_fn = match uri.extension() {
            Some(Extension::Json) => Self::from_json,
            Some(Extension::Toml) => Self::from_toml,
            Some(Extension::Yaml) => Self::from_yaml,
            Some(Extension::Unknown(extension)) => bail!(
                "Failed to read quickwit config file `{}`: file extension `.{}` is not supported. \
                 Supported file formats and extensions are JSON (.json), TOML (.toml), and YAML \
                 (.yaml or .yml).",
                uri,
                extension
            ),
            None => bail!(
                "Failed to read config file `{}`: file extension is missing. Supported file \
                 formats and extensions are JSON (.json), TOML (.toml), and YAML (.yaml or .yml).",
                uri
            ),
        };
        let rendered_config = render_config(uri, config_content)?;
        parser_fn(rendered_config.as_bytes())
    }

    fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_reader(StripComments::new(bytes))
            .context("Failed to parse JSON config file.")
    }

    fn from_toml(bytes: &[u8]) -> anyhow::Result<Self> {
        toml::from_slice(bytes).context("Failed to parse TOML config file.")
    }

    fn from_yaml(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_yaml::from_slice(bytes).context("Failed to parse YAML config file.")
    }

    // fn parse_services(&self) -> anyhow::Result<HashSet<QuickwitService>> {
    //     self.enabled_services
    //         .iter()
    //         .map(|service_string| service_string.parse())
    //         .collect::<Result<HashSet<_>, _>>()
    // }

    // /// Returns the REST listen address of the node, i.e. the socket address on which the REST
    // API /// service listens for TCP connections.
    // async fn rest_listen_addr(&self, listen_host: &Host) -> anyhow::Result<SocketAddr> {
    //     listen_host.with_port(self.rest_listen_port).resolve().await
    // }

    // /// Returns the gRPC listen port of the node.
    // /// Extracted from the config if specified or computed as `rest_listen_port + 1` otherwise.
    // fn grpc_listen_port(&self) -> u16 {
    //     self.grpc_listen_port.unwrap_or(self.rest_listen_port + 1)
    // }

    // /// Returns the gRPC listen address of the node, i.e. the socket address on which the gRPC
    // /// service listens for TCP connections.
    // async fn grpc_listen_addr(&self, listen_host: &Host) -> anyhow::Result<SocketAddr> {
    //     listen_host
    //         .with_port(self.grpc_listen_port())
    //         .resolve()
    //         .await
    // }

    /// Returns the advertise
    fn advertise_addr(&self, listen_host: &Host) -> anyhow::Result<Host> {
        if let Ok(advertise_address) = env::var("QW_ADVERTISE_ADDRESS") {
            return advertise_address.parse().map(|addr| {
                info!(advertise_address=%advertise_address, "Using advertise address from environment variable `QW_ADVERTISE_ADDRESS`.");
                addr
            }).with_context(|| {
                format!(
                    "Failed to parse advertise address `{advertise_address}` read from \
                     environment variable `QW_ADVERTISE_ADDRESS`."
                )
            });
        }
        if let Some(advertise_addr) = &self.advertise_address {
            return advertise_addr.parse().map(|addr| {
                info!(advertise_address=%advertise_addr, "Using advertise address from config file.");
                addr
            }).with_context(|| {
                format!(
                    "Failed to parse advertise address `{advertise_addr}` read from \
                     config file."
                )
            });
        }
        if listen_host.is_unspecified() {
            if let Some((interface_name, private_ip)) = find_private_ip() {
                info!(advertise_address=%private_ip, interface_name=%interface_name, "Using sniffed advertise address.");
                return Ok(Host::from(private_ip));
            }
            bail!(
                "Listen address `{}` is unspecified and advertise address is not set.",
                listen_host
            );
        }
        info!(advertise_address=%listen_host, "Using listen address as advertise address.");
        Ok(listen_host.clone())
    }

    // /// Returns the gRPC public address of the node, i.e. the socket address to connect to in
    // order /// to send gRPC requests to the node.
    // async fn grpc_advertise_addr(&self, listen_host: &Host) -> anyhow::Result<SocketAddr> {
    //     self.advertise_addr(listen_host)?
    //         .with_port(self.grpc_listen_port())
    //         .resolve()
    //         .await
    // }

    // /// Returns the gossip listen port of the node (UDP).
    // /// Extracted from the config if specified or same as `rest_listen_port` otherwise.
    // fn gossip_listen_port(&self) -> u16 {
    //     // By default, we use the same port number as the REST port but UDP this time.
    //     self.gossip_listen_port.unwrap_or(self.rest_listen_port)
    // }

    // /// Returns the gossip listen address of the node, i.e. the UDP socket address on which the
    // node /// receives gossip messages.
    // async fn gossip_listen_addr(&self, listen_addr: &Host) -> anyhow::Result<SocketAddr> {
    //     listen_addr
    //         .with_port(self.gossip_listen_port())
    //         .resolve()
    //         .await
    // }

    // /// Returns the gossip public address of the node, i.e. the socket address to send UDP
    // packets /// to in order to gossip with the node.
    // async fn gossip_advertise_addr(&self, listen_host: &Host) -> anyhow::Result<SocketAddr> {
    //     self.advertise_addr(listen_host)?
    //         .with_port(self.gossip_listen_port())
    //         .resolve()
    //         .await
    // }

    // fn default_index_root_uri(&self) -> anyhow::Result<Uri> {
    //     if let Some(uri) = &self.default_index_root_uri {
    //         Uri::try_new(uri)
    //             .with_context(|| format!("Failed to parse default index root URI `{uri}`."))
    //     } else {
    //         Ok(default_index_root_uri(&self.data_dir_path))
    //     }
    // }

    // fn data_dir_path(&self) -> anyhow::Result<PathBuf> {
    //     let data_dir_uri = Uri::try_new(&self.data_dir_path.to_string_lossy())
    //         .expect("Failed to create URI from data_dir.");
    //     match data_dir_uri.filepath() {
    //         Some(path) => Ok(path.to_path_buf()),
    //         _ => bail!("Only `file://` protocol allowed for data_dir `{data_dir_uri}`."),
    //     }
    // }

    pub async fn build(self, env_vars: &HashMap<String, String>) -> anyhow::Result<QuickwitConfig> {
        let listen_address = self.listen_address.resolve(&env_vars)?;
        let listen_host = listen_address.parse::<Host>()?;
        let listen_ip = listen_host.resolve().await?;

        let rest_listen_port = self.rest_listen_port.resolve(&env_vars)?;
        let rest_listen_addr = SocketAddr::new(listen_ip, rest_listen_port);

        let gossip_listen_port = self
            .gossip_listen_port
            .map(|port| port.resolve(&env_vars))
            .transpose()?
            .unwrap_or(rest_listen_port);
        let gossip_listen_addr = SocketAddr::new(listen_ip, gossip_listen_port);

        let grpc_listen_port = self
            .grpc_listen_port
            .map(|port| port.resolve(&env_vars))
            .transpose()?
            .unwrap_or(rest_listen_port + 1);
        let grpc_listen_addr = SocketAddr::new(listen_ip, grpc_listen_port);

        // let advertise_address = self.listen_address.resolve(&env_vars)?;
        // let advertise_host = listen_address.parse::<Host>()?;
        let advertise_ip = listen_ip;
        let gossip_advertise_addr = SocketAddr::new(advertise_ip, gossip_listen_port);
        let grpc_advertise_addr = SocketAddr::new(advertise_ip, grpc_listen_port);

        let enabled_services = self
            .enabled_services
            .resolve(&env_vars)?
            .0
            .into_iter()
            .map(|service| service.parse())
            .collect::<Result<_, _>>()?;

        let data_dir_path = self.data_dir_path.resolve(&env_vars)?;
        let data_dir_uri = Uri::try_new(&data_dir_path.to_string_lossy())
            .with_context(|| format!("Failed to parse data dir: `{}`.", data_dir_path.display()))?;
        let data_dir_path = data_dir_uri
            .filepath()
            .with_context(|| format!(""))?
            .to_path_buf();

        let metastore_uri = self
            .metastore_uri
            .map(|uri| uri.resolve(&env_vars))
            .transpose()?
            .unwrap_or_else(|| default_metastore_uri(&data_dir_uri));

        let default_index_root_uri = self
            .default_index_root_uri
            .map(|uri| uri.resolve(&env_vars))
            .transpose()?
            .unwrap_or_else(|| default_index_root_uri(&data_dir_uri));

        Ok(QuickwitConfig {
            version: self.version.resolve(&env_vars)?,
            cluster_id: self.cluster_id.resolve(&env_vars)?,
            node_id: self.node_id.resolve(&env_vars)?,
            enabled_services,
            rest_listen_addr,
            gossip_listen_addr,
            grpc_listen_addr,
            gossip_advertise_addr,
            grpc_advertise_addr,
            peer_seeds: self.peer_seeds.resolve(&env_vars)?.0,
            data_dir_path,
            metastore_uri,
            default_index_root_uri,
            indexer_config: self.indexer_config,
            searcher_config: self.searcher_config,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct QuickwitConfig {
    pub version: usize,
    pub cluster_id: String,
    pub node_id: String,
    pub enabled_services: HashSet<QuickwitService>,
    pub rest_listen_addr: SocketAddr,
    pub gossip_listen_addr: SocketAddr,
    pub grpc_listen_addr: SocketAddr,
    pub gossip_advertise_addr: SocketAddr,
    pub grpc_advertise_addr: SocketAddr,
    pub peer_seeds: Vec<String>,
    pub data_dir_path: PathBuf,
    pub metastore_uri: Uri,
    pub default_index_root_uri: Uri,
    pub indexer_config: IndexerConfig,
    pub searcher_config: SearcherConfig,
}

impl QuickwitConfig {
    /// Parses and validates a [`QuickwitConfig`] from a given URI and config content.
    pub async fn load(
        uri: &Uri,
        config_content: &[u8],
        _data_dir_path_opt: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let config_builder = QuickwitConfigBuilder::from_uri(uri, config_content).await?;
        let env_vars = env::vars().collect::<HashMap<_, _>>();
        let config = config_builder.build(&env_vars).await?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        validate_identifier("Cluster ID", &self.cluster_id)?;
        validate_identifier("Node ID", &self.node_id)?;

        if self.cluster_id == DEFAULT_CLUSTER_ID {
            warn!(
                cluster_id=%DEFAULT_CLUSTER_ID,
                "Cluster ID is not set, falling back to default value."
            );
        }
        if self.peer_seeds.is_empty() {
            warn!("Peer seed list is empty.");
        }
        let data_dir_uri = Uri::try_new(&self.data_dir_path.to_string_lossy())?;

        if !data_dir_uri.protocol().is_file() {
            bail!(
                "Data dir must be located on local file system. Current location: `{data_dir_uri}`"
            )
        }
        if !self.data_dir_path.exists() {
            bail!(
                "Data dir `{}` does not exist.",
                self.data_dir_path.display()
            );
        }
        Ok(())
    }

    /// Returns the list of peer seed addresses. The addresses MUST NOT be resolved. Otherwise, the
    /// DNS-based discovery mechanism implemented in Chitchat will not work correctly.
    pub async fn peer_seed_addrs(&self) -> anyhow::Result<Vec<String>> {
        let mut peer_seed_addrs = Vec::new();
        let default_gossip_port = self.gossip_listen_addr.port();

        // We want to pass non-resolved addresses to Chitchat but still want to resolve them for
        // validation purposes. Additionally, we need to append a default port if necessary and
        // finally return the addresses as strings, which is tricky for IPv6. We let the logic baked
        // in `HostAddr` handle this complexity.
        for peer_seed in &self.peer_seeds {
            let peer_seed_addr = HostAddr::parse_with_default_port(peer_seed, default_gossip_port)?;
            if let Err(error) = peer_seed_addr.resolve().await {
                warn!(peer_seed = %peer_seed_addr, error = ?error, "Failed to resolve peer seed address.");
                continue;
            }
            peer_seed_addrs.push(peer_seed_addr.to_string())
        }
        if !self.peer_seeds.is_empty() && peer_seed_addrs.is_empty() {
            bail!(
                "Failed to resolve any of the peer seed addresses: `{}`",
                self.peer_seeds.join(", ")
            )
        }
        Ok(peer_seed_addrs)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        let enabled_services = QuickwitService::supported_services();

        let listen_address = Host::default();
        let rest_listen_port = quickwit_common::net::find_available_tcp_port()
            .expect("The OS should almost always find an available port.");
        let rest_listen_addr = listen_address
            .with_port(rest_listen_port)
            .to_socket_addr()
            .expect("The default host should be an IP address.");
        let gossip_listen_addr = listen_address
            .with_port(rest_listen_port)
            .to_socket_addr()
            .expect("The default host should be an IP address.");
        let grpc_listen_port = quickwit_common::net::find_available_tcp_port()
            .expect("The OS should almost always find an available port.");
        let grpc_listen_addr = listen_address
            .with_port(grpc_listen_port)
            .to_socket_addr()
            .expect("The default host should be an IP address.");

        let data_dir_path = default_data_dir_path().value;
        let data_dir_uri = Uri::try_new(DEFAULT_DATA_DIR_PATH)
            .expect("The default data dir path should be a valid URI.");
        let metastore_uri = default_metastore_uri(&data_dir_uri);
        let default_index_root_uri = default_index_root_uri(&data_dir_uri);

        Self {
            version: 0,
            cluster_id: default_cluster_id().value,
            node_id: default_node_id().value,
            enabled_services,
            gossip_advertise_addr: gossip_listen_addr,
            grpc_advertise_addr: grpc_listen_addr,
            rest_listen_addr,
            gossip_listen_addr,
            grpc_listen_addr,
            peer_seeds: Vec::new(),
            data_dir_path,
            metastore_uri,
            default_index_root_uri,
            indexer_config: IndexerConfig::default(),
            searcher_config: SearcherConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;

    impl Default for QuickwitConfigBuilder {
        fn default() -> Self {
            Self {
                version: ConfigValue::default(),
                cluster_id: default_cluster_id(),
                node_id: default_node_id(),
                enabled_services: default_enabled_services(),
                listen_address: default_listen_address(),
                rest_listen_port: default_rest_listen_port(),
                gossip_listen_port: None,
                grpc_listen_port: None,
                advertise_address: None,
                peer_seeds: ConfigValue::default(),
                metastore_uri: None,
                default_index_root_uri: None,
                data_dir_path: default_data_dir_path(),
                indexer_config: IndexerConfig::default(),
                searcher_config: SearcherConfig::default(),
            }
        }
    }

    fn get_config_filepath(config_filename: &str) -> String {
        format!(
            "{}/resources/tests/config/{}",
            env!("CARGO_MANIFEST_DIR"),
            config_filename
        )
    }

    macro_rules! test_parser {
        ($test_function_name:ident, $file_extension:expr) => {
            #[tokio::test]
            async fn $test_function_name() -> anyhow::Result<()> {
                let config_filepath =
                    get_config_filepath(&format!("quickwit.{}", stringify!($file_extension)));
                let config_uri = Uri::try_new(&config_filepath)?;
                let file = std::fs::read_to_string(&config_filepath).unwrap();
                let config = QuickwitConfigBuilder::from_uri(&config_uri, file.as_bytes())
                    .await?
                    .build(&HashMap::new())
                    .await?;
                assert_eq!(config.version, 0);
                assert_eq!(config.cluster_id, "quickwit-cluster");
                assert_eq!(
                    config.rest_listen_addr,
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 1111)
                );
                assert_eq!(
                    config.peer_seeds,
                    vec![
                        "quickwit-searcher-0.local".to_string(),
                        "quickwit-searcher-1.local".to_string()
                    ]
                );
                assert_eq!(config.enabled_services.len(), 2);
                assert!(config.enabled_services.contains(&QuickwitService::Janitor));
                assert!(config
                    .enabled_services
                    .contains(&QuickwitService::Metastore));
                assert_eq!(
                    config.metastore_uri,
                    "postgres://username:password@host:port/db"
                );

                assert_eq!(
                    config.indexer_config,
                    IndexerConfig {
                        enable_opentelemetry_otlp_service: false,
                        split_store_max_num_bytes: Byte::from_str("1T").unwrap(),
                        split_store_max_num_splits: 10_000,
                        max_concurrent_split_uploads: 8,
                    }
                );

                assert_eq!(
                    config.searcher_config,
                    SearcherConfig {
                        enable_jaeger_service: false,
                        fast_field_cache_capacity: Byte::from_str("10G").unwrap(),
                        split_footer_cache_capacity: Byte::from_str("1G").unwrap(),
                        max_num_concurrent_split_searches: 150,
                        max_num_concurrent_split_streams: 120,
                    }
                );

                Ok(())
            }
        };
    }

    test_parser!(test_config_from_json, json);
    test_parser!(test_config_from_toml, toml);
    test_parser!(test_config_from_yaml, yaml);

    #[tokio::test]
    async fn test_config_contains_wrong_values() {
        let config_filepath = get_config_filepath("quickwit.wrongkey.yaml");
        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let config_str = std::fs::read_to_string(&config_filepath).unwrap();
        let parsing_error = QuickwitConfigBuilder::from_uri(&config_uri, config_str.as_bytes())
            .await
            .unwrap_err();
        assert!(format!("{parsing_error:?}")
            .contains("unknown field `max_num_concurrent_split_searchs`"));
    }

    #[test]
    fn test_indexer_config_default_values() {
        let indexer_config = serde_yaml::from_str::<IndexerConfig>("{}").unwrap();
        assert_eq!(indexer_config, IndexerConfig::default());
    }

    #[test]
    fn test_searcher_config_default_values() {
        let searcher_config = serde_yaml::from_str::<SearcherConfig>("{}").unwrap();
        assert_eq!(searcher_config, SearcherConfig::default());
    }

    #[tokio::test]
    async fn test_quickwit_config_default_values_minimal() {
        let config_yaml = "version: 0";
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build(&HashMap::new()).await.unwrap();
        assert_eq!(config.version, 0);
        assert_eq!(config.cluster_id, DEFAULT_CLUSTER_ID);
        assert!(config.node_id.starts_with("node-"));
        assert_eq!(
            config.enabled_services,
            QuickwitService::supported_services()
        );
        assert_eq!(
            config.metastore_uri,
            format!(
                "file://{}/qwdata/indexes#polling_interval=30s",
                env::current_dir().unwrap().display()
            )
        );
        assert_eq!(
            config.data_dir_path.to_string_lossy(),
            format!("{}/qwdata", env::current_dir().unwrap().display())
        );
    }

    #[tokio::test]
    async fn test_quickwwit_config_default_values_storage() {
        let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
        "#;
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build(&HashMap::new()).await.unwrap();
        assert_eq!(config.version, 0);
        assert_eq!(config.cluster_id, DEFAULT_CLUSTER_ID);
        assert_eq!(config.node_id, "1");
        assert_eq!(
            config.metastore_uri,
            "postgres://username:password@host:port/db"
        );
    }

    #[tokio::test]
    async fn test_quickwit_config_config_default_values_default_indexer_searcher_config() {
        let config_yaml = r#"
            version: 0
            metastore_uri: postgres://username:password@host:port/db
            data_dir: /opt/quickwit/data
        "#;
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build(&HashMap::new()).await.unwrap();
        assert_eq!(config.version, 0);
        assert_eq!(
            config.metastore_uri,
            "postgres://username:password@host:port/db"
        );
        assert_eq!(config.indexer_config, IndexerConfig::default());
        assert_eq!(config.searcher_config, SearcherConfig::default());
    }

    #[tokio::test]
    async fn test_quickwit_config_validate() {
        let config_filepath = get_config_filepath("quickwit.toml");
        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let file_content = std::fs::read_to_string(&config_filepath).unwrap();
        let data_dir_path = env::current_dir().unwrap();
        let config =
            QuickwitConfig::load(&config_uri, file_content.as_bytes(), Some(data_dir_path))
                .await
                .unwrap();
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_peer_socket_addrs() {
        {
            let quickwit_config = QuickwitConfigBuilder {
                rest_listen_port: ConfigValue::new(1789),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert!(quickwit_config.peer_seed_addrs().await.unwrap().is_empty());
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                rest_listen_port: ConfigValue::new(1789),
                peer_seeds: ConfigValue::new(List(vec!["unresolvable-host".to_string()])),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert!(quickwit_config.peer_seed_addrs().await.is_err());
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                rest_listen_port: ConfigValue::new(1789),
                peer_seeds: ConfigValue::new(List(vec![
                    "unresolvable-host".to_string(),
                    "localhost".to_string(),
                    "localhost:1337".to_string(),
                    "127.0.0.1".to_string(),
                    "127.0.0.1:1337".to_string(),
                ])),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.peer_seed_addrs().await.unwrap(),
                vec![
                    "localhost:1789".to_string(),
                    "localhost:1337".to_string(),
                    "127.0.0.1:1789".to_string(),
                    "127.0.0.1:1337".to_string()
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_socket_addr_ports() {
        {
            let quickwit_config = QuickwitConfigBuilder {
                listen_address: default_listen_address(),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.rest_listen_addr.to_string(),
                "127.0.0.1:7280"
            );
            assert_eq!(
                quickwit_config.gossip_listen_addr.to_string(),
                "127.0.0.1:7280"
            );
            assert_eq!(
                quickwit_config.grpc_listen_addr.to_string(),
                "127.0.0.1:7281"
            );
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                listen_address: default_listen_address(),
                rest_listen_port: ConfigValue::new(1789),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.rest_listen_addr.to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.gossip_listen_addr.to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.grpc_listen_addr.to_string(),
                "127.0.0.1:1790"
            );
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                listen_address: default_listen_address(),
                rest_listen_port: ConfigValue::new(1789),
                gossip_listen_port: Some(ConfigValue::new(1889)),
                grpc_listen_port: Some(ConfigValue::new(1989)),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.rest_listen_addr.to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.gossip_listen_addr.to_string(),
                "127.0.0.1:1889"
            );
            assert_eq!(
                quickwit_config.grpc_listen_addr.to_string(),
                "127.0.0.1:1989"
            );
        }
    }

    #[tokio::test]
    async fn test_load_config_with_validation_error() {
        let config_filepath = get_config_filepath("quickwit.yaml");
        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let file = std::fs::read_to_string(&config_filepath).unwrap();
        let config = QuickwitConfig::load(&config_uri, file.as_bytes(), None)
            .await
            .unwrap_err();
        assert!(config.to_string().contains("Data dir"));
    }

    #[tokio::test]
    async fn test_config_validates_uris() {
        {
            let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: ''
        "#;
            serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap_err();
        }
        {
            let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
            default_index_root_uri: ''
        "#;
            serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap_err();
        }
    }

    #[tokio::test]
    async fn test_quickwit_config_data_dir_accepts_both_file_uris_and_file_paths() {
        {
            let config_yaml = r#"
                version: 0
                data_dir: /opt/quickwit/data
            "#;
            let config_builder =
                serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
            let config = config_builder.build(&HashMap::new()).await.unwrap();
            assert_eq!(config.data_dir_path, PathBuf::from("/opt/quickwit/data"));
        }
        {
            let config_yaml = r#"
                version: 0
                data_dir: file:///opt/quickwit/data
            "#;
            let config_builder =
                serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
            let config = config_builder.build(&HashMap::new()).await.unwrap();
            assert_eq!(config.data_dir_path, PathBuf::from("/opt/quickwit/data"));
        }
        {
            let config_yaml = r#"
                version: 0
                data_dir: s3://indexes/foo
            "#;
            let config_builder =
                serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
            let error = config_builder.build(&HashMap::new()).await.unwrap_err();
            assert!(error.to_string().contains("Only `file://` protocol"));
        }
    }
}
