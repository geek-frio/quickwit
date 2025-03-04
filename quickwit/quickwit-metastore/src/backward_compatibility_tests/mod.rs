// Copyright (C) 2023 Quickwit, Inc.
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

use std::fs;
use std::path::Path;

use anyhow::{bail, Context};
use quickwit_config::TestableForRegression;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::file_backed_metastore::file_backed_index::FileBackedIndex;
use crate::{IndexMetadata, SplitMetadata};

/// In order to avoid confusion, we need to make sure that the
/// resource versions is the same for all resources.
///
/// We don't want to confuse quickwit users with different source_config /
/// index_config versions.
///
/// If you bump this version, makes sure to update all resources.
/// Of course some resource may not have any config change.
///
/// You can just reuse the same versioned object in that case.
/// ```
/// enum MyResource {
///     #[serde(rename="0.1")]
///     V0_1(MyResourceV1),
///     #[serde(rename="0.2")]
///     V0_2(MyResourceV1) //< there was no change in this version.
/// }
const GLOBAL_QUICKWIT_RESOURCE_VERSION: &str = "0.4";

/// This test makes sure that the resource is using the current `GLOBAL_QUICKWIT_RESOURCE_VERSION`.
fn test_global_version<T: Serialize>(serializable: &T) -> anyhow::Result<()> {
    let json = serde_json::to_value(serializable).unwrap();
    let version_value = json.get("version").context("No version tag")?;
    let version_str = version_value.as_str().context("version should be a str")?;
    if version_str != GLOBAL_QUICKWIT_RESOURCE_VERSION {
        bail!(
            "Version `{version_str}` is not the global quickwit resource version \
             ({GLOBAL_QUICKWIT_RESOURCE_VERSION})"
        );
    }
    Ok(())
}

fn deserialize_json_file<T>(path: &Path) -> anyhow::Result<T>
where for<'a> T: Deserialize<'a> {
    let payload = std::fs::read(path)?;
    let deserialized: T = serde_json::from_slice(&payload)?;
    Ok(deserialized)
}

fn test_backward_compatibility_single_case<T>(path: &Path) -> anyhow::Result<()>
where T: TestableForRegression {
    println!("---\nTest deserialization of {}", path.display());
    let deserialized: T = deserialize_json_file(path)?;
    let expected_path = path.to_string_lossy().replace(".json", ".expected.json");
    let expected: T = deserialize_json_file(Path::new(&expected_path))?;
    deserialized.test_equality(&expected);
    Ok(())
}

fn test_backward_compatibility<T>(test_dir: &Path) -> anyhow::Result<()>
where T: TestableForRegression {
    for entry in
        fs::read_dir(test_dir).with_context(|| format!("Failed to read {}", test_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.to_string_lossy().ends_with(".expected.json") {
            continue;
        }
        test_backward_compatibility_single_case::<T>(&path)
            .with_context(|| format!("test path {}", path.display()))?;
    }
    Ok(())
}

fn test_and_update_expected_files_single_case<T>(expected_path: &Path) -> anyhow::Result<bool>
where for<'a> T: Serialize + Deserialize<'a> {
    let expected: T = deserialize_json_file(Path::new(&expected_path))?;
    let expected_old_json_value: JsonValue = deserialize_json_file(Path::new(&expected_path))?;
    let expected_new_json_value: JsonValue = serde_json::to_value(&expected)?;
    // We compare json Value, so we don't detect format change like a change in the field order.
    if expected_old_json_value == expected_new_json_value {
        // No modification
        return Ok(false);
    }
    let mut expected_new_json = serde_json::to_string_pretty(&expected_new_json_value)?;
    expected_new_json.push('\n');
    std::fs::write(expected_path, expected_new_json.as_bytes())?;
    Ok(true)
}

fn test_and_update_expected_files<T>(test_dir: &Path) -> anyhow::Result<()>
where for<'a> T: Deserialize<'a> + Serialize {
    let mut updated_expected_files = Vec::new();
    for entry in fs::read_dir(test_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.to_string_lossy().ends_with(".expected.json") {
            continue;
        }
        if test_and_update_expected_files_single_case::<T>(&path)
            .with_context(|| format!("test filepath {}", path.display()))?
        {
            updated_expected_files.push(path);
        }
    }
    assert!(
        updated_expected_files.is_empty(),
        "The following expected files need to be updated. {updated_expected_files:?}"
    );
    Ok(())
}

fn test_and_create_new_test<T>(test_dir: &Path, sample: T) -> anyhow::Result<()>
where for<'a> T: Serialize {
    let sample_json_value = serde_json::to_value(&sample)?;
    let version: &str = sample_json_value
        .as_object()
        .unwrap()
        .get("version")
        .expect("missing version")
        .as_str()
        .expect("version should be a string");
    let mut sample_json = serde_json::to_string_pretty(&sample_json_value)?;
    sample_json.push('\n');
    let md5_digest = md5::compute(&sample_json);
    let test_name = format!("v{version}-{md5_digest:x}");
    let file_regression_test_path = format!("{}/{}.json", test_dir.display(), test_name);
    let file_regression_expected_path =
        format!("{}/{}.expected.json", test_dir.display(), test_name);
    std::fs::write(file_regression_test_path, sample_json.as_bytes())?;
    std::fs::write(file_regression_expected_path, sample_json.as_bytes())?;
    Ok(())
}

/// This helper function scans the `test-data/{test_name}`
/// for JSON deserialization regression tests and runs them sequentially.
///
/// - `test_name` is just the subdirectory name, for the type being test.
/// - `test` is a function asserting the equality of the deserialized version
/// and the expected version.
pub(crate) fn test_json_backward_compatibility_helper<T>(test_name: &str) -> anyhow::Result<()>
where T: TestableForRegression {
    let sample_instance: T = T::sample_for_regression();
    let test_dir = Path::new("test-data").join(test_name);
    test_global_version(&sample_instance).context("Version is not the global version.")?;
    test_backward_compatibility::<T>(&test_dir).context("backward-compatibility")?;
    test_and_update_expected_files::<T>(&test_dir).context("test-and-update")?;
    test_and_create_new_test::<T>(&test_dir, sample_instance)
        .context("test-and-create-new-test")?;
    Ok(())
}

#[test]
fn test_split_metadata_backward_compatibility() {
    test_json_backward_compatibility_helper::<SplitMetadata>("split-metadata").unwrap();
}

#[test]
fn test_index_metadata_backward_compatibility() {
    test_json_backward_compatibility_helper::<IndexMetadata>("index-metadata").unwrap();
}

#[test]
fn test_file_backed_index_backward_compatibility() {
    test_json_backward_compatibility_helper::<FileBackedIndex>("file-backed-index").unwrap();
}
