use std::fmt;
use std::any::Any;

use crate::stringpool;


pub struct Column {
    pub name: Option<String>,
    dtype: Dtype,
    pub data: Box<dyn VectorData>,
}

impl Column {
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

// impl PartialEq for Column {

//     fn eq(&self, other: &Rhs) -> bool {
//         true
//     }

//     fn ne(&self, other: &Rhs) -> bool {
//         false
//     }

// }




pub trait VectorData {
    fn push_from_str(&mut self, x: &str);
    fn to_string(&self) -> String;
    fn as_any(&self) -> &dyn Any;
    fn reserve(&mut self, addtional: usize);
    fn boxed_clone(&self) -> Box<dyn VectorData>;
    fn dtype(&self) -> Dtype;
}

impl fmt::Display for dyn VectorData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub enum Dtype {
    ColInt,
    ColDouble,
    ColIntNullable,
    ColDoubleNullable,
    ColString,
    ColStringPool
}

impl Dtype {
    pub fn from_str(s: &str) -> Dtype {
        match s {
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
}

//vanilla i32 f32 vectors
#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct ColInt {pub data: Vec<i32>}

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
    fn push_from_str(&mut self, x: &str) {
        self.data.push(x.trim().parse::<i32>().unwrap())
    }
    fn to_string(&self) -> String {
        self.data.iter()
        .map(|x| x.to_string())
        .fold(String::new(), |a,b| a+&b[..]+", ".into())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn reserve(&mut self, addtional: usize)  {
        self.data.reserve(addtional);
    }
    fn boxed_clone(&self) -> Box<dyn VectorData> {
        let y = self.clone();
        let x: Box<dyn VectorData> = Box::new(y);
        x
    }
    fn dtype(&self) -> Dtype {
        Dtype::ColInt
    }
}
impl VectorData for ColDouble {
    fn push_from_str(&mut self, x: &str) {
        self.data.push(x.trim().parse::<f32>().unwrap()) //f32 only diff fom above
    }
    fn to_string(&self) -> String {
        self.data.iter()
        .map(|x| x.to_string())
        .fold(String::new(), |a,b| a+&b[..]+", ".into())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn reserve(&mut self, addtional: usize)  {
        self.data.reserve(addtional);
    }
    fn boxed_clone(&self) -> Box<dyn VectorData> {
        let y = self.clone();
        let x: Box<dyn VectorData> = Box::new(y);
        x
    }
    fn dtype(&self) -> Dtype {
        Dtype::ColDouble
    }
}

//implement nullable 
impl VectorData for ColIntNullable {
    fn push_from_str(&mut self, x: &str) {
        self.data.push(x.trim().parse::<i32>().ok())
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
    fn reserve(&mut self, addtional: usize)  {
        self.data.reserve(addtional);
    }
    fn boxed_clone(&self) -> Box<dyn VectorData> {
        let y = self.clone();
        let x: Box<dyn VectorData> = Box::new(y);
        x
    }
    fn dtype(&self) -> Dtype {
        Dtype::ColIntNullable
    }
}

impl VectorData for ColDoubleNullable {
    fn push_from_str(&mut self, x: &str) {
        self.data.push(x.trim().parse::<f32>().ok()) //f32 only diff from above
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
    fn reserve(&mut self, addtional: usize)  {
        self.data.reserve(addtional);
    }
    fn boxed_clone(&self) -> Box<dyn VectorData> {
        let y = self.clone();
        let x: Box<dyn VectorData> = Box::new(y);
        x
    }
    fn dtype(&self) -> Dtype {
        Dtype::ColDouble
    }
}

impl VectorData for ColString {
    fn push_from_str(&mut self, x: &str) {
        self.data.push(String::from(x))
    }
    fn to_string(&self) -> String {
        self.data.iter()
        .fold(String::new(), |a,b| a+&b[..]+", ".into())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn reserve(&mut self, addtional: usize)  {
        self.data.reserve(addtional);
    }
    fn boxed_clone(&self) -> Box<dyn VectorData> {
        let y = self.clone();
        let x: Box<dyn VectorData> = Box::new(y);
        x
    }
    fn dtype(&self) -> Dtype {
        Dtype::ColString
    }
}

impl VectorData for ColStringPool {
    fn push_from_str(&mut self, x: &str) {
        self.data.add_str(x);
    }
    fn to_string(&self) -> String {
        self.data.to_string()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn reserve(&mut self, addtional: usize)  {
        self.data.pool.reserve(addtional);
    }
    fn boxed_clone(&self) -> Box<dyn VectorData> {
        let y = self.clone();
        let x: Box<dyn VectorData> = Box::new(y);
        x
    }
    fn dtype(&self) -> Dtype {
        Dtype::ColStringPool
    }
}



