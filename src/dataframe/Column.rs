use std::fmt;
use std::any::Any;
use std::borrow::Cow;
use crate::stringpool;

//use super::Result;
use std::error;
type Result<Column> = std::result::Result<Column, Box<dyn error::Error>>;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ColErrorcode {
    ParseDataType,
    SchemaSyntax,
}

#[derive(Debug, Clone)]
pub struct ColError {
    pub errorcode: ColErrorcode,
    pub error_msg: String,
}

impl fmt::Display for ColError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.errorcode {
            ColErrorcode::ParseDataType => {
                write!(f, "failed to parse \"{}\" to any known datatype", self.error_msg)?;
            },
            ColErrorcode::SchemaSyntax => {
                write!(f, "failed to parse the token stream \"{}\" as a column", self.error_msg)?;
            }
        }
        Ok(())
    }
}
impl error::Error for ColError {}


//Column has optional name, a data-type flag and dynamic trait type VectorData
pub struct Column  {
    pub name: Option<String>,
    dtype: Dtype,
    pub data: Box<dyn VectorData >,
}
impl  Column  {
    pub fn new(
        name: Option<String>,
        dtype: Dtype
    ) -> Column {
        let data: Box<dyn VectorData> = match dtype {
            Dtype::ColInt => Box::new(ColInt::default()),
            Dtype::ColDouble => Box::new(ColDouble::default()),
            Dtype::ColIntNullable => Box::new(ColIntNullable::default()),
            Dtype::ColDoubleNullable => Box::new(ColDoubleNullable::default()),
            Dtype::ColString => Box::new(ColString::default()),
            Dtype::ColStringPool => Box::new(ColStringPool::default()),
        };
        Column{name,dtype,data}
    }
}
impl Clone for Column {
    fn clone(&self) -> Self {
        Column{
            name: self.name.clone(),
            dtype: self.dtype,
            data: self.data.boxed_clone(),
        }

    }
}

//VectorData is the trait that data of any column has, be it ints or floats or something else.
pub trait VectorData {
    fn push_from_str(&mut self, x: &str) -> Result<()>;
    fn to_string(&self) -> String;
    fn as_any(& self) -> &dyn Any;
    fn reserve(&mut self, additional: usize);
    fn dtype(&self) -> Dtype;
    fn boxed_clone(&self) -> Box<dyn VectorData>;
}
impl fmt::Display for dyn VectorData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}


//Dtype is the column flag of data type.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub enum Dtype {
    ColInt,
    ColDouble,
    ColIntNullable,
    ColDoubleNullable,
    ColString,
    ColStringPool, //stringpool crate
}
impl fmt::Display for Dtype{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Dtype::ColInt =>  write!(f, "DType: ColInt"),
            Dtype::ColDouble =>  write!(f, "DType: ColDouble"),
            Dtype::ColIntNullable =>  write!(f, "DType: ColIntNullable"),
            Dtype::ColDoubleNullable =>  write!(f, "DType: ColDoubleNullable"),
            Dtype::ColString =>  write!(f, "DType: ColString"),
            Dtype::ColStringPool =>  write!(f, "DType: ColStringPool"),
        }
    }
}

impl Dtype {
    pub fn from_str(s: &str) -> Dtype {
        let lc_s = s.to_lowercase();
        match &lc_s[..] {
            "int" => Dtype::ColInt,
            "intnullable" => Dtype::ColIntNullable,
            "double" => Dtype::ColDouble,
            "doublenullable" => Dtype::ColDoubleNullable,
            "string" => Dtype::ColString,
            "stringpool" => Dtype::ColStringPool,
            _ => {
                panic!("could not parse dtype {}",s);
            },
        }
    }

    pub fn from_str_to_res(s: &str) -> Result<Dtype> {
        let lc_s = s.to_lowercase();
        let new_dtype = match &lc_s[..] {
            "int" => Dtype::ColInt,
            "intnullable" => Dtype::ColIntNullable,
            "double" => Dtype::ColDouble,
            "doublenullable" => Dtype::ColDoubleNullable,
            "string" => Dtype::ColString,
            "stringpool" => Dtype::ColStringPool,
            _ => {
                let err = ColError{
                    errorcode: ColErrorcode::ParseDataType,
                    error_msg: s.to_string(),
                
                };
                return Err(Box::new(err))
            }
        };
        Ok(new_dtype)
    }
}


//all structs that implement VectorData

//vanilla i32 f32 vectors
#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct ColInt {pub data: Vec<i32>}

#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct ColIntCow<'a>{
    pub data: Cow<'a,Vec<i32>>
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct ColDouble {pub data: Vec<f32>}

//nullable, vectors of optional i32 and f32
#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct ColIntNullable {pub data: Vec<Option<i32>>}

#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct ColDoubleNullable {pub data: Vec<Option<f32>>}

//string vector

#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct ColString {pub data: Vec<String>}


#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct ColStringPool {pub data: stringpool::StringPool}


//implement vanilla i32 and f32 vectors
impl VectorData for ColInt {
    fn push_from_str(&mut self, x: &str) -> Result<()> {
        let value = x.trim().parse::<i32>()?;
        self.data.push(value);
        Ok(())
    }
    fn to_string(&self) -> String {
        self.data.iter()
        .map(|x| x.to_string())
        .fold(String::new(), |a,b| a+&b[..]+", ".into())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn reserve(&mut self, additional: usize)  {
        self.data.reserve(additional);
    }

    fn boxed_clone(&self) -> Box<dyn VectorData> {
        Box::new(self.clone())
    }

    fn dtype(&self) -> Dtype {
        Dtype::ColInt
    }
}

impl VectorData  for ColDouble {
    fn push_from_str(&mut self, x: &str) -> Result<()> {
        let value = x.trim().parse::<f32>()?;
        self.data.push(value);
        Ok(())
    }
    fn to_string(&self) -> String {
        self.data.iter()
        .map(|x| x.to_string())
        .fold(String::new(), |a,b| a+&b[..]+", ".into())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn reserve(&mut self, additional: usize)  {
        self.data.reserve(additional);
    }
    fn boxed_clone(&self) -> Box<dyn VectorData> {
        Box::new(self.clone())
    }
    fn dtype(&self) -> Dtype {
        Dtype::ColDouble
    }
}

//implement nullable 
impl  VectorData  for ColIntNullable {
    fn push_from_str(&mut self, x: &str) -> Result<()> {
        self.data.push(x.trim().parse::<i32>().ok());
        Ok(())
    }
    fn to_string(&self) -> String {
        self.data.iter()
        .map(|x| {
            if let Some(y) = x {
                y.to_string()
            } else {
                String::from("NA")
            }
        })
        .fold(String::new(), |a,b| a+&b[..]+", ".into())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn reserve(&mut self, additional: usize)  {
        self.data.reserve(additional);
    }
    fn boxed_clone(&self) -> Box<dyn VectorData> {
        Box::new(self.clone())
    }
    fn dtype(&self) -> Dtype {
        Dtype::ColIntNullable
    }
}

impl  VectorData for ColDoubleNullable {
    fn push_from_str(&mut self, x: &str) -> Result<()> {
        self.data.push(x.trim().parse::<f32>().ok()); //f32 only diff from above
        Ok(())
    }
    fn to_string(&self) -> String {
        self.data[..].iter()
        .map(|x| {
            if let Some(y) = x {
                y.to_string()
            } else {
                String::from("NA")
            }
        })
        .fold(String::new(), |a,b| a+&b[..]+", ".into())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn reserve(&mut self, additional: usize)  {
        self.data.reserve(additional);
    }
    fn boxed_clone(&self) -> Box<dyn VectorData> {
        Box::new(self.clone())
    }
    fn dtype(&self) -> Dtype {
        Dtype::ColDouble
    }
}

impl VectorData for ColString {
    fn push_from_str(&mut self, x: &str) -> Result<()> {
        self.data.push(String::from(x));
        Ok(())
    }
    fn to_string(&self) -> String {
        self.data.iter()
        .fold(String::new(), |a,b| a+&b[..]+", ".into())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn reserve(&mut self, additional: usize)  {
        self.data.reserve(additional);
    }
    fn boxed_clone(&self) -> Box<dyn VectorData> {
        Box::new(self.clone())
    }

    fn dtype(&self) -> Dtype {
        Dtype::ColString
    }
}

impl VectorData for ColStringPool {
    fn push_from_str(&mut self, x: &str) -> Result<()> {
        self.data.add_str(x);
        Ok(())
    }
    fn to_string(&self) -> String {
        self.data.to_string()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn reserve(&mut self, additional: usize)  {
        self.data.reserve(additional);
    }
    fn boxed_clone(&self) -> Box<dyn VectorData> {
        Box::new(self.clone())
    }
    fn dtype(&self) -> Dtype {
        Dtype::ColStringPool
    }
}