use std::sync::Arc;

use clap::Subcommand;
use iceberg::{Catalog, NamespaceIdent};

#[derive(Subcommand, Debug, PartialEq)]
pub enum Command {
    List,
    Get { name: String },
    Create { name: String },
    Delete { name: String },
    Rename { from: String, to: String },
}

pub async fn exec(catalog: Arc<dyn Catalog>, command: &Option<Command>) -> anyhow::Result<()> {
    match command {
        // No subcommand defaults to list
        None | Some(Command::List) => {
            println!("Listing namespaces");
            let namespaces = catalog.list_namespaces(None).await?;
            s
            println!("Namespaces: {:?}", namespaces);
        }
        Some(Command::Get { name }) => {
            let ident = NamespaceIdent::new(name.clone());
            let namespace = catalog.get_namespace(&ident).await?;
            println!("Namespace: {:?}", namespace);
        }
        _ => todo!(),
    }
    Ok(())
}
