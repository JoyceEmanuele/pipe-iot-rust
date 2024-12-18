use serde_cbor;

// fn get_dac_state_globs(dev_id: &str, conv: &mut ConversionVars, hw_cfg: &HwInfoDAC) -> Result<Box<L1Calculator>, String> {
//     let dac_state_list = &mut conv.dac_state_list;
//     let dac_state = dac_state_list.get_or_create(&dev_id, hw_cfg);
//     Ok(Box::new(dac_state))
// }

pub fn serialize_state_obj<T>(dev_state: T) -> Result<Vec<u8>, String>
where
    T: serde::Serialize,
{
    let dev_state_bytes = serde_cbor::to_vec(&dev_state).map_err(|err| err.to_string())?;
    return Ok(dev_state_bytes);
}
