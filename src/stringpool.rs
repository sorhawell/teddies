use std::fmt;
use std::ptr;

const POOL_STRING_SIZE: usize = 1024;

#[derive(Clone, Debug, PartialEq, PartialOrd, Hash, Eq, Ord, Default)]
pub struct StringTicket {
    start: usize,
    len: usize,
    i_chunk: usize,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Hash, Eq, Ord)]
pub struct StringPool {
    v: Vec<StringTicket>,
    pool: Vec<String>,
}

impl StringPool {

    pub fn new() -> StringPool {
        StringPool {
            v: Vec::new(),
            pool: vec![String::with_capacity(POOL_STRING_SIZE)]
        }
    }

    pub fn get_last_idx(&self) -> usize {
        self.pool.len()-1 as usize
    }

    fn borrow_last_chunk(&mut self) -> &mut String {  
        let i_chunk = self.get_last_idx();
        &mut self.pool[i_chunk]
    }

    pub fn add_str(&mut self, s: &str) {
            let mut i_chunk = self.pool.len()-1 as usize;
            let mut lc = &mut self.pool[i_chunk];
            
            let mut chars_used = lc.len() as usize;
            if chars_used >= POOL_STRING_SIZE {
                self.pool.push(String::with_capacity(POOL_STRING_SIZE));
                i_chunk += 1;
                lc = &mut self.pool[i_chunk];
                chars_used = 0;
            };

            //push s to chunk
            lc.push_str(s);
            self.v.push(StringTicket{start:chars_used, len:s.len(), i_chunk}) 
    }

    pub fn get_str(& self, idx: usize) -> &str {
        let st = &self.v[idx];
        &self.pool[st.i_chunk][st.start..st.start+st.len]
    }

    pub fn reserve(&mut self, additional: usize)  {
        self.borrow_last_chunk().reserve(additional);
    }

}

impl Default for StringPool {
    fn default() -> StringPool {
        StringPool::new()
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

        println!("new stringpool must always have one pool ready");
        assert!(sp.pool.len()==1);
        assert!(sp.get_last_idx() == 0); //with idx 0

        println!("however the number of inserted strings should be 0");
        assert!(sp.v.len()==0);

        sp.add_str("hello");
        sp.add_str(" world");

        assert_eq!(sp.get_str(0),"hello");
        assert_eq!(sp.get_str(1)," world");
   }

   #[test]
   fn hello_alot() {
        let mut sp = StringPool::new();

        let mut temp_string = String::new();
        for i in 0..POOL_STRING_SIZE {
            temp_string = format!(" hello_world_{}", i);
            sp.add_str(&temp_string);
        }

        println!("inserted and retrived str match");
        for i in 0..POOL_STRING_SIZE {
            assert_eq!(
                sp.get_str(i),
                format!(" hello_world_{}", i)
            );
            
        }

        let mut sp_cloned = sp.clone();
        println!("clone must not point to same address");
        assert!(
            !ptr::eq(
                sp.borrow_last_chunk(),
                sp_cloned.borrow_last_chunk()
            )
        );

        println!("cloned content is the same");
        for i in 0..POOL_STRING_SIZE {
            assert_eq!(
                sp.get_str(i),
                sp_cloned.get_str(i),
            );

        }
        
   
    }

    #[test]
    fn reserve_ps() {
        //make string pool with some chunks
        let mut sp = StringPool::new();
        let mut temp_string = String::new();
        for i in 0..POOL_STRING_SIZE {
            temp_string = format!(" hello_world_{}", i);
            sp.add_str(&temp_string);
        }

        //get capicity, and reserve to exceed capacity
        let lc_cap_before  = sp.borrow_last_chunk().capacity();
        let lc_len = sp.borrow_last_chunk().len();
        let enough_to_trigger_reallocation = lc_cap_before - lc_len + 42;
        sp.reserve(enough_to_trigger_reallocation);
        let lc_cap_after  = sp.borrow_last_chunk().capacity();

        //check new capacity is bigger
        println!("using reserve exceeding capacity resulted in increased capacity");
        println!("bf:{} af:{} and len{}", lc_cap_before, lc_cap_after, lc_len);
        assert!(lc_cap_after - 42 >= lc_cap_before);
    }


}