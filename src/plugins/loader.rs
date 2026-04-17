use std::sync::Arc;

use serde::Serialize;

use crate::domain::interfaces::Validator;
use crate::recognition::registry::RecognizerRegistry;
use crate::validation::checks::default_validators;

#[derive(Clone, Debug, Serialize)]
pub struct PluginManifest {
    pub plugin_id: String,
    pub version: String,
    pub recognizers: Vec<String>,
    pub validators: Vec<String>,
}

#[derive(Clone)]
pub struct PluginContext {
    pub validators: Vec<Arc<dyn Validator>>,
    pub manifests: Vec<PluginManifest>,
}

pub fn load_plugins(plugin_ids: &[String], registry: &mut RecognizerRegistry) -> PluginContext {
    let mut validators = default_validators()
        .into_iter()
        .map(Arc::<dyn Validator>::from)
        .collect::<Vec<_>>();
    let mut manifests = Vec::new();

    for plugin_id in plugin_ids {
        match plugin_id.as_str() {
            "sample_builtin_pack" => {
                let (manifest, plugin_validators) =
                    crate::plugins::sample_plugin::register(registry);
                manifests.push(manifest);
                validators.extend(plugin_validators);
            }
            _ => {}
        }
    }

    PluginContext {
        validators,
        manifests,
    }
}
