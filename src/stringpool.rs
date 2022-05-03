use std::fmt;

#[derive(Clone, Debug, PartialEq, PartialOrd, Hash, Eq, Ord, Default)]
pub struct StringTicket {
    start: usize,
    len: usize,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Hash, Eq, Ord, Default)]
pub struct StringPool {
    pub v: Vec<StringTicket>,
    pub pool: String,
}

impl StringPool {

    pub fn new() -> StringPool {
        StringPool {v: Vec::new(), pool: String::new() }
    }

    pub fn add_str(&mut self, s: &str) {
            let nchars_before = self.pool.len();
            self.pool.push_str(s);
            self.v.push(StringTicket{start:nchars_before, len:s.len()}) 
    }

    pub fn get_str(& self, idx: usize) -> &str {
        let st = &self.v[idx];
        &self.pool[st.start..st.start+st.len]
    }

    pub fn repr(& self) {
        for i in 0..self.v.len() {
            println!("{}",self.get_str(i));
        }
    }
}

impl fmt::Display for StringPool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"[")?;
        for i in 0..self.v.len() {
            write!(f," \"{}\",",self.get_str(i))?;
        }
        write!(f,"]")?;
        Ok(())
    }
}



#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn try_add_str() {
        let mut sp = StringPool::new();

        sp.add_str("hello");
        sp.add_str(" world");

        sp.repr();
        println!("{}",sp);

        assert_eq!(sp.get_str(0),"hello");
        assert_eq!(sp.get_str(1)," world");
   }


}