use uuid::Uuid;

use crate::io::FileIO;
use crate::spec::TableMetadata;
use crate::{Error, ErrorKind, Result};

struct MetadataWriter {
    file_io: FileIO,
    metadata_dir: String,
}

impl MetadataWriter {
    pub fn new(file_io: FileIO, dir: &str) -> Self {
        Self {
            file_io,
            metadata_dir: dir.trim_end_matches('/').to_string(),
        }
    }

    pub async fn read(&self, path: &str) -> Result<TableMetadata> {
        let content = self.file_io.new_input(path)?.read().await?;
        Ok(serde_json::from_slice::<TableMetadata>(&content)?)
    }

    /// Writes a metadata file of version 0 to the file IO, and returns its location.
    pub async fn write_new(&self, metadata: TableMetadata) -> Result<String> {
        self.write_metadata(metadata, 0).await
    }

    pub async fn write_new_version(
        &self,
        metadata: TableMetadata,
        current_path: &str,
    ) -> Result<String> {
        let (path, file_name) = current_path.split_once('/')?;
        if path.trim_end_matches('/') != self.metadata_dir {
            return Err(Error::new(ErrorKind::DataInvalid, "Metadata path mismatch"));
        }

        let current_version = parse_version(file_name)?;
        let new_version = current_version + 1;

        self.write_metadata(metadata, new_version).await
    }

    async fn write_metadata(&self, metadata: TableMetadata, version: u32) -> Result<String> {
        let file_name = generate_metadata_file_name(version);
        let path = format!("{}/{}", self.metadata_dir, file_name);

        self.file_io
            .new_output(&path)?
            .write(serde_json::to_vec(&metadata)?.into())
            .await?;

        Ok(path)
    }
}

fn generate_metadata_file_name(version: u32) -> String {
    let uuid = Uuid::new_v4();
    format!("{}-{}.metadata.json", version, uuid)
}

/// Parses a file of the format `<version>-<uuid>.metadata.json` and returns the
/// version. It returns an error if the file does not match the expected format.
fn parse_version(file_name: &str) -> Result<u32> {
    let (name, extension) = file_name.split_once('.').ok_or(Error::new(
        ErrorKind::DataInvalid,
        "Invalid metadata file name",
    ))?;
    if extension != "metadata.json" {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid metadata file extension {}", extension),
        ));
    }

    let (version_str, id_str) = name.split_once('-').ok_or(Error::new(
        ErrorKind::DataInvalid,
        "Invalid metadata file name",
    ))?;

    id_str.parse::<Uuid>()?; // Verify that the id is a valid UUID
    let version = version_str.parse::<u32>()?;

    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fail() {
        assert!(parse_version("1234567890.metadata.json").is_err());
        assert!(parse_version("1234567890-1234567890.metadata.json").is_err());
        assert!(parse_version("1234567890-1234567890.metadata.json").is_err());
        assert!(false);
    }
}
