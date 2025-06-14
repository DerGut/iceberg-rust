use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_cli::exec;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::{MaxRows, PrintOptions};

use crate::IcebergCatalogList;

// TODO: should take df specific args
pub async fn exec(
    catalogs: &IcebergCatalogList,
    format: PrintFormat,
    quiet: bool,
    maxrows: MaxRows,
    color: bool,
) -> anyhow::Result<()> {
    let session_config = SessionConfig::from_env()?.with_information_schema(true);

    let rt_builder = RuntimeEnvBuilder::new();

    let runtime_env = rt_builder.build_arc()?;

    // enable dynamic file query
    let ctx = SessionContext::new_with_config_rt(session_config, runtime_env).enable_url_table();
    ctx.refresh_catalogs().await?;

    let mut print_options = PrintOptions {
        format,
        quiet,
        maxrows,
        color,
    };

    let providers = catalogs.try_to_provider_list().await?;
    ctx.register_catalog_list(providers);

    Ok(exec::exec_from_repl(&ctx, &mut print_options).await?)
}
