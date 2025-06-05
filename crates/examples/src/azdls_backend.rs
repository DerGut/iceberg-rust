use std::collections::HashMap;

use futures::StreamExt;
use iceberg::io::FileIO;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_memory::MemoryCatalog;

const SAS_TOKEN: &str = "<my-sas-token>";
const ADLS_BASE_PATH: &str = "abfss://<fsname>@<account>.dfs.core.windows.net";

#[tokio::main]
async fn main() {
    let file_io = FileIO::from_path(ADLS_BASE_PATH)
        .unwrap()
        .with_prop(iceberg::io::ADLS_SAS_TOKEN, SAS_TOKEN)
        .build()
        .unwrap();
    let catalog = MemoryCatalog::new(file_io, None);

    // Create the table identifier.
    let namespace_ident = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), "example".to_string());

    if !catalog.namespace_exists(&namespace_ident).await.unwrap() {
        println!("Namespace {namespace_ident} does not exist, creating it now.");
        catalog
            .create_namespace(&namespace_ident, HashMap::new())
            .await
            .unwrap();
    }

    // Check if the table exists.
    if !catalog.table_exists(&table_ident).await.unwrap() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "age", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .build()
            .unwrap();

        let creation = TableCreation::builder()
            .name("example".to_string())
            .location(ADLS_BASE_PATH.to_string() + "/test/example")
            .schema(schema)
            .build();

        catalog
            .create_table(&namespace_ident, creation)
            .await
            .unwrap();
    }

    let table = catalog.load_table(&table_ident).await.unwrap();
    println!("Table {table_ident} loaded!");

    let scan = table.scan().select_all().build().unwrap();
    let reader = scan.to_arrow().await.unwrap();
    let buf = reader.collect::<Vec<_>>().await;

    println!("Table {table_ident} has {} batches.", buf.len());

    assert!(buf.is_empty());
    assert!(buf.iter().all(|x| x.is_ok()));
}
