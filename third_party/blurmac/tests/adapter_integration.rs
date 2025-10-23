use blurmac::BluetoothAdapter;
use std::error::Error;

// These tests don't really check much beyond whether the calls succeed,
// as the actual values may vary depending on the system and environment.
// Better than nothing though.

#[test]
fn test_adapter_basic_info() -> Result<(), Box<dyn Error>> {
    let adapter = BluetoothAdapter::init()?;

    let id = adapter.get_id();
    assert!(!id.is_empty(), "get_id should not return empty string");

    let name = adapter.get_name()?;
    assert!(!name.is_empty(), "get_name should not return empty string");

    let address = adapter.get_address()?;
    assert!(!address.is_empty(), "get_address should not return empty string");

    adapter.get_class()?;

    Ok(())
}

#[test]
fn test_adapter_bool_properties() -> Result<(), Box<dyn Error>> {
    let adapter = BluetoothAdapter::init()?;

    adapter.is_powered()?;
    adapter.is_discoverable()?;

    Ok(())
}
