use std::fmt;
use std::any::Any;

pub struct Column {
    pub name: Option<String>,
    pub data: Box<dyn VectorData>,
}

pub trait VectorData {
    fn push_from_str(&mut self, x: &str);
    fn to_string(&self) -> String;
    fn as_any(&self) -> &dyn Any;
}

impl fmt::Display for dyn VectorData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

//vanilla i32 f32 vectors
pub struct ColInt {pub data: Vec<i32>}
pub struct ColDouble {pub data: Vec<f32>}

//nullable, vectors of optional i32 and f32
pub struct ColIntNullable {pub data: Vec<Option<i32>>}
pub struct ColDoubleNullable {pub data: Vec<Option<f32>>}

//string vector
pub struct ColString {pub data: Vec<String>}

//implement vanilla i32 and f32 vectors
impl VectorData for ColInt {
    fn push_from_str(&mut self, x: &str) {
        self.data.push(x.parse::<i32>().unwrap())
    }
    fn to_string(&self) -> String {
        self.data.iter()
        .map(|x| x.to_string())
        .fold(String::new(), |a,b| a+&b[..]+", ".into())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
impl VectorData for ColDouble {
    fn push_from_str(&mut self, x: &str) {
        self.data.push(x.parse::<f32>().unwrap()) //f32 only diff fom above
    }
    fn to_string(&self) -> String {
        self.data.iter()
        .map(|x| x.to_string())
        .fold(String::new(), |a,b| a+&b[..]+", ".into())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

//implement nullable 
impl VectorData for ColIntNullable {
    fn push_from_str(&mut self, x: &str) {
        self.data.push(x.parse::<i32>().ok())
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
}

impl VectorData for ColDoubleNullable {
    fn push_from_str(&mut self, x: &str) {
        self.data.push(x.parse::<f32>().ok()) //f32 only diff from above
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
}
