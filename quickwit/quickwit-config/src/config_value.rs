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

use std::collections::HashMap;
use std::str::FromStr;
use std::{any, fmt};

use anyhow;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::qw_env_vars::{QW_ENV_VARS, QW_NONE};

#[derive(Clone, Default, Eq, PartialEq)]
pub(crate) struct ConfigValue<T, const E: usize> {
    pub value: T,
}

impl<T, const E: usize> fmt::Debug for ConfigValue<T, E>
where T: fmt::Debug
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}

impl<T, const E: usize> fmt::Display for ConfigValue<T, E>
where T: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl<T, const E: usize> ConfigValue<T, E>
where
    T: FromStr,
    <T as FromStr>::Err: fmt::Debug,
{
    pub(crate) fn new(value: T) -> Self {
        Self { value }
    }

    pub(crate) fn resolve(self, env_vars: &HashMap<String, String>) -> anyhow::Result<T> {
        // QW env vars take precedence over the config file values.
        if E > QW_NONE {
            if let Some(env_var_key) = QW_ENV_VARS.get(&E) {
                if let Some(env_var_value) = env_vars.get(*env_var_key) {
                    let value = env_var_value.parse::<T>().map_err(|error| {
                        anyhow::anyhow!(
                            "Failed to convert value `{env_var_value}` read from environment \
                             variable `{env_var_key}` to type `{}`: {error:?}",
                            any::type_name::<T>(),
                        )
                    })?;
                    return Ok(value);
                }
            }
        }
        Ok(self.value)
    }
}

impl<'de, T, const E: usize> Deserialize<'de> for ConfigValue<T, E>
where T: Deserialize<'de>
{
    fn deserialize<D>(deserializer: D) -> Result<ConfigValue<T, E>, D::Error>
    where D: Deserializer<'de> {
        T::deserialize(deserializer).map(|value| ConfigValue { value })
    }
}

impl<T, const E: usize> Serialize for ConfigValue<T, E>
where T: Serialize
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        self.value.serialize(serializer)
    }
}
