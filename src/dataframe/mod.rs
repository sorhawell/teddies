use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

pub mod column;
pub mod lineparser;
use crate::stringpool;
use std::fmt;

use std::str;
use filebuffer::FileBuffer;

extern crate filebuffer;


pub struct DataFrame {
    pub data: Vec<column::Column>,
}

impl fmt::Display for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s: String = self.data.iter()
        .map(|x| {
            String::new() +
            x.name.as_ref().unwrap_or(&String::from("None")) +
            ": " +
            &x.data.to_string()[..]
        })
        .fold(String::new(), |a,b| a+&b[..]+"\n".into());
        write!(f, "DataFrame\n{}", s)
    }
}

impl Clone for DataFrame {
    fn clone(&self) -> Self {
        DataFrame{data: self.data.to_vec()}
    }
}


impl DataFrame {
    pub fn new(schema: &str) -> DataFrame {
        
        let split_chars: &[char] = &[',', ';', '\n'][..];
        
        let columns: Vec<column::Column> = schema
            
            .split(split_chars) // split into column descriptions
            
            .map(|raw_col_description| {

                //remove case and split into tokens by ':'
                let lcase_col_description = raw_col_description.to_lowercase();
                let mut tokens: Vec<&str> = lcase_col_description.split(":").collect();

                //pop last token and parse to dtype-flag
                let dtype = column::Dtype::from_str(
                    tokens.pop().unwrap()
                );
                
                //optional first token as optional name
                let mut name: Option<String> = None;
                if let Some(x) = tokens.pop() {
                    name = Some(String::from(x));
                }

                //no more tokens should be seen
                if tokens.pop().is_some() {
                    panic!("extra tokens found for {}", raw_col_description);
                };

                //create column by dtype
                column::Column::new(name, dtype)

            })
            .collect();

            DataFrame{
                data: columns
            }
    }


    pub fn append_line(&mut self, i_line: &str) {
        
        if i_line.len() == 0 {
            return;
        }

        //split by delimiter, iterator of segments
        let mut line_iter = i_line.split(',');

        //loop over columns in data frame
        for i_column in 0..self.data.len() {
            
            //consume str value from line
            let cell_str = line_iter
                .next()
                .unwrap_or("")
                .trim();

            //push into column, appropriate parsing applied if columns are e.g. i32, f32 vectors.
            self.data[i_column].data.push_from_str(cell_str);
        }

    }

    pub fn append_str(&mut self, text: &str) {
        
        let csvstr = lineparser::CsvStr::new(text, 0, 0);
        let mut last_row: usize = 0;
        let mut last_col: usize = 0;
        for i in csvstr {
            if i.row != last_row {
                //add to remaining columns if missing cells in line
                if last_col < self.data.len() - 1  {
                    for _j in (last_col+1)..self.data.len() {
                        self.data[i.col].data.push_from_str("");
                    }
                }
            }
            //add cell to df
            self.data[i.col].data.push_from_str(i.text);
            last_col = i.col;
            last_row = i.row;
        }
    }

    pub fn reserve(&mut self, addtional: usize) {
        for i in 0..self.data.len() {
            self.data[i].data.reserve(addtional);
        }
    }

}



fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}


pub fn csv_read_file_fast(file_name: &str, schema_str: &str) -> DataFrame {

    let x = String::from(schema_str);
    //make empty data frame by schema str
    let mut df = DataFrame::new(&x);


    if let Ok(fbuffer) = FileBuffer::open(&file_name) {
        let text =
            str::from_utf8(&fbuffer)
            .expect("not valid UTF-8");

        let lines = text.lines(); 

        for i_line in lines {       
            // try read a line, skip if empty line
            df.append_line(&i_line);
        }
    }

    return df
}


pub fn csv_read_file_iter(file_name: &str, schema_str: &str) -> DataFrame {

    let x = String::from(schema_str);
    //make empty data frame by schema str
    let mut df = DataFrame::new(&x);

    if let Ok(fbuffer) = FileBuffer::open(&file_name) {
        let text =
            str::from_utf8(&fbuffer)
            .expect("not valid UTF-8");

        // //prepass to infer line count    
        // let n_lines = text.lines().count();
        // df.reserve(n_lines + 500);
     
        // try read a line, skip if empty line
        df.append_str(text);
      
    }

    return df
}

pub fn csv_read_file(file_name: &str, schema_str: &str) -> DataFrame {

    let x = String::from(schema_str);
    //make empty data frame by schema str
    let mut df = DataFrame::new(&x);


    //if reading succeeded
    if let Ok(lines) = read_lines(file_name) {
        
        for i_line in lines {
            
            // try read a line, skip if empty line
            let i_line = i_line.unwrap();

            df.append_line(&i_line);

        }
    }

    return df
}

pub fn csv_read_str(csv_str: &str, schema_str: &str) -> DataFrame {

    let x = String::from(schema_str);
    //make empty data frame by schema str
    let mut df = DataFrame::new(&x);

        
    for line in csv_str.lines() {
        df.append_line(line);
    }

    return df
}

#[allow(dead_code)]
pub fn csv_read_str_iter(csv_str: &str, schema_str: &str) -> DataFrame {

    let x = String::from(schema_str);
    //make empty data frame by schema str
    let mut df = DataFrame::new(&x);

    df.append_str(csv_str);

    return df
}

// pub fn csv_read_str_par(csv_str: &str, schema_str: &str) -> DataFrame {

//     let x = String::from(schema_str);
//     //make empty data frame by schema str
//     let mut df = DataFrame::new(&x);

//     let ts = csv_str.split_at(csv_str.len()/2);
//     let s = [ts.0,ts.1];

//     s.map(|x| x.split())
    
//     for i in s.iter() {


//     }

//     df.append_str(csv_str);

//     return df
// }



#[cfg(test)]
mod tests {
   use super::*;

    #[test]
    fn test_csv_read_str() {
    //read this csv by this schema 
    let mycsvstr = "1 ,2.2 , 3,four\n10, 20.20,30,fourty";
    let myschema = "a:int,b:doubleNullable,c:int,someothername:string";
    let df = csv_read_str(mycsvstr, myschema);

    //check ColInt
    let a_ref_col = Box::new(column::ColInt{data: vec![1,10]});
    let a_act_col: &column::ColInt = &df.data[0].data
        .as_any()
        .downcast_ref::<column::ColInt>()
        .expect("not expected column type");
    assert_eq!(
        *a_act_col.data,
        *a_ref_col.data
    );

    //check ColDoubleNullablet
    let b_ref_col = Box::new(
        column::ColDoubleNullable{data: vec![Some(2.2),Some(20.20)]}
    );
    let b_act_col: &column::ColDoubleNullable = &df.data[1].data
        .as_any()
        .downcast_ref::<column::ColDoubleNullable>()
        .expect("not expected column type");
    assert_eq!(
        *b_act_col.data,
        *b_ref_col.data
    );

    //check ColString
    let d_ref_col = Box::new(
        column::ColString{data: vec![String::from("four"), String::from("fourty")]}
    );
    let d_act_col: &column::ColString = &df.data[3].data
        .as_any()
        .downcast_ref::<column::ColString>()
        .expect("not expected column type");
    assert_eq!(
        *d_act_col.data,
        *d_ref_col.data
    );
    }

    #[test]
    fn test_csv_read_iter() {
        //read this csv by this schema 
        let mycsvstr = "1 ,2.2 , 3,four\n10, 20.20,30,fourty";
        let myschema = "a:int,b:doubleNullable,c:int,someothername:string";
        let df = csv_read_str_iter(mycsvstr, myschema);

        println!("iter df looks like ({})",df);
    
        //check ColInt
        let a_ref_col = Box::new(column::ColInt{data: vec![1,10]});
        let a_act_col: &column::ColInt = &df.data[0].data
            .as_any()
            .downcast_ref::<column::ColInt>()
            .expect("not expected column type");
        assert_eq!(
            *a_act_col.data,
            *a_ref_col.data
        );
    
        //check ColDoubleNullablet
        let b_ref_col = Box::new(
            column::ColDoubleNullable{data: vec![Some(2.2),Some(20.20)]}
        );
        let b_act_col: &column::ColDoubleNullable = &df.data[1].data
            .as_any()
            .downcast_ref::<column::ColDoubleNullable>()
            .expect("not expected column type");
        assert_eq!(
            *b_act_col.data,
            *b_ref_col.data
        );
    
        //check ColString
        let d_ref_col = Box::new(
            column::ColString{data: vec![String::from("four"), String::from("fourty")]}
        );
        let d_act_col: &column::ColString = &df.data[3].data
            .as_any()
            .downcast_ref::<column::ColString>()
            .expect("not expected column type");
        assert_eq!(
            *d_act_col.data,
            *d_ref_col.data
        );

    }

}
