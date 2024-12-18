use rusoto_dynamodb::{AttributeValue};
use std::str::FromStr;

// "B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"
// "BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]
// "NULL": true

// "M": {"Name": {"S": "Joe"}, "Age": {"N": "35"}}

// "BOOL": true
// "N": "123.45"
// "S": "Hello"

// "L": [ {"S": "Cookies"} , {"S": "Coffee"}, {"N", "3.14159"}]
// "NS": ["42.2", "-19", "7.5", "3.14"]
// "SS": ["Giraffe", "Hippo" ,"Zebra"]

pub fn get_float_number_array_prop(prop_o: &Option<&AttributeValue>, array_length: usize) -> Result<Vec<Option<f64>>,String> {
    let prop_a = match prop_o { Some(a) => a, None => return Ok(vec![None;array_length]) };
    if let Some(ref vec) = prop_a.l {
        let mut arr = Vec::<Option<f64>>::new();
        arr.reserve(vec.len());
        for prop_a in vec {
            if let Some(ref val_str) = prop_a.n {
                match f64::from_str(val_str) {
                    Ok(v) => arr.push(Some(v)),
                    Err(err) => { crate::LOG.append_log_tag_msg("ERROR", &format!("{}", err)); return Err("ERROR76".to_owned()); }
                }
            } else {
                arr.push(None);
            }
        }
        return Ok(arr)
    }
    if let Some(ref vec) = prop_a.ns {
        let mut arr = Vec::<Option<f64>>::new();
        arr.reserve(vec.len());
        for val_str in vec {
            match f64::from_str(val_str) {
                Ok(v) => arr.push(Some(v)),
                Err(err) => { crate::LOG.append_log_tag_msg("ERROR", &format!("{}", err)); return Err("ERROR90".to_owned()); }
            }
        }
        return Ok(arr)
    }
    if let Some(ref val_str) = prop_a.n {
        let mut arr = Vec::<Option<f64>>::new();
        match f64::from_str(val_str) {
            Ok(v) => arr.push(Some(v)),
            Err(err) => { crate::LOG.append_log_tag_msg("ERROR", &format!("{}", err)); return Err("ERROR99".to_owned()); }
        }
        return Ok(arr)
    }
    return Err("Could not find valid attribute value".to_owned());
}

pub fn get_int_number_array_prop(prop_o: &Option<&AttributeValue>, array_length: usize) -> Result<Vec<Option<i16>>,String> {
    let prop_a = match prop_o { Some(a) => a, None => return Ok(vec![None;array_length]) };
    if let Some(ref vec) = prop_a.l {
        let mut arr = Vec::<Option<i16>>::new();
        arr.reserve(vec.len());
        for prop_a in vec {
            if let Some(ref val_str) = prop_a.n {
                match i16::from_str(val_str) {
                    Ok(v) => arr.push(Some(v)),
                    Err(err) => { crate::LOG.append_log_tag_msg("ERROR", &format!("{}", err)); return Err("ERROR115".to_owned()); }
                }
            } else {
                arr.push(None);
            }
        }
        return Ok(arr)
    }
    if let Some(ref vec) = prop_a.ns {
        let mut arr = Vec::<Option<i16>>::new();
        arr.reserve(vec.len());
        for val_str in vec {
            match i16::from_str(val_str) {
                Ok(v) => arr.push(Some(v)),
                Err(err) => { crate::LOG.append_log_tag_msg("ERROR", &format!("{}", err)); return Err("ERROR129".to_owned()); }
            }
        }
        return Ok(arr)
    }
    if let Some(ref val_str) = prop_a.n {
        let mut arr = Vec::<Option<i16>>::new();
        match i16::from_str(val_str) {
            Ok(v) => arr.push(Some(v)),
            Err(err) => { crate::LOG.append_log_tag_msg("ERROR", &format!("{}", err)); return Err("ERROR138".to_owned()); }
        }
        return Ok(arr)
    }
    return Err("Could not find valid attribute value".to_owned());
}

pub fn get_bool_array_prop(prop_o: &Option<&AttributeValue>, array_length: usize) -> Result<Vec<Option<bool>>,String> {
    let prop_a = match prop_o { Some(a) => a, None => return Ok(vec![None;array_length]) };
    if let Some(ref vec) = prop_a.l {
        let mut arr = Vec::<Option<bool>>::new();
        arr.reserve(vec.len());
        for prop_a in vec {
            if let Some(ref val_str) = prop_a.n {
                if val_str == "1" { arr.push(Some(true)); }
                else if val_str == "0" { arr.push(Some(false)); }
                else { arr.push(None); }
            } else if let Some(val_bool) = prop_a.bool {
                if val_bool { arr.push(Some(true)); }
                else { arr.push(Some(false)); }
            } else {
                arr.push(None);
            }
        }
        return Ok(arr)
    }
    if let Some(ref vec) = prop_a.ns {
        let mut arr = Vec::<Option<bool>>::new();
        arr.reserve(vec.len());
        for val_str in vec {
            if val_str == "1" { arr.push(Some(true)); }
            else if val_str == "0" { arr.push(Some(false)); }
            else { arr.push(None); }
        }
        return Ok(arr)
    }
    if let Some(ref val_str) = prop_a.n {
        let mut arr = Vec::<Option<bool>>::new();
        if val_str == "1" { arr.push(Some(true)); }
        else if val_str == "0" { arr.push(Some(false)); }
        else { arr.push(None); }
        return Ok(arr)
    }
    if let Some(val_bool) = prop_a.bool {
        let mut arr = Vec::<Option<bool>>::new();
        if val_bool { arr.push(Some(true)); }
        else { arr.push(Some(false)); }
        return Ok(arr)
    }
    return Err("Could not find valid attribute value".to_owned());
}

pub fn get_string_prop(prop_o: &Option<&AttributeValue>) -> Result<String,String> {
    match prop_o {
      None => Err("Attribute is empty".to_owned()),
      Some(prop_a) => match &prop_a.s {
          Some(value) => Ok(value.to_owned()),
          None => Err("Could not find valid attribute value".to_owned())
      },
  }
}

pub fn get_int_number_prop(prop_o: &Option<&AttributeValue>) -> Result<i64,String> {
    match prop_o {
      None => Err("Attribute is empty".to_owned()),
      Some(prop_a) => match &prop_a.n {
          Some(val_str) => {
            match i64::from_str(val_str) {
                Ok(v) => Ok(v),
                Err(err) => Err(format!("ERROR158: {}", err)),
            }
          },
          None => Err("Could not find valid attribute value".to_owned())
      },
  }
}

pub fn get_array_length(prop_o: &Option<&AttributeValue>) -> Result<Option<usize>, String> {
    let prop_a = match prop_o { Some(a) => a, None => return Ok(None) };
    if let Some(ref vec) = prop_a.l {
        return Ok(Some(vec.len()));
    }
    if let Some(ref vec) = prop_a.ns {
        return Ok(Some(vec.len()));
    }
    if let Some(ref _val_str) = prop_a.s {
        return Ok(Some(1));
    }
    if let Some(ref _val_str) = prop_a.n {
        return Ok(Some(1));
    }
    if let Some(_val_bool) = prop_a.bool {
        return Ok(Some(1));
    }
    if let Some(is_null) = prop_a.null {
        if is_null { return Ok(None); }
    }
    return Err("Could not find valid attribute value [258]".to_owned());
}
