use tinyconnectors_bus::{CollectionSpec as ContractCollectionSpec, STORAGE_NEEDS};
use tinystoragedrivers_bus::{CollectionSpec, StorageNeeds};

fn main() {
    let needs: &StorageNeeds<'static> = &STORAGE_NEEDS;
    let _: &tinyconnectors_bus::StorageNeeds<'static> = needs;
    assert_eq!(needs.collections.len(), 1);
    let collection: &CollectionSpec = &needs.collections[0];
    let _: &ContractCollectionSpec = collection;
    assert_eq!(
        collection,
        &CollectionSpec {
            name: "connectors.user_scopes".into(),
            indexes: Vec::new(),
            ttl_field: None,
            search: None,
        }
    );
    assert!(needs.blob_prefixes.is_empty());
    assert_eq!(needs.stream_prefixes, ["connectors.triggers/"]);
    assert!(needs.secret_prefixes.is_empty());
}
