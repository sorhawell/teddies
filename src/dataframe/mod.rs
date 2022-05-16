use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

pub mod column;
pub mod lineparser;
use std::fmt;

use std::str;
use filebuffer::FileBuffer;

extern crate filebuffer;

use std::error;
type Result<T> = std::result::Result<T, Box<dyn error::Error>>;


#[derive(Debug)]
pub struct DFError {
    error_msg: String,
    sub_errors: Vec<Box<dyn error::Error>>,
}
impl fmt::Display for DFError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "dataframe error {}", self.error_msg)?;
        for (i, err) in self.sub_errors.iter().enumerate() {
            write!(f, "\nsub error {}: {}",i, err.to_string())?;
        }
        Ok(())
    }
}
impl error::Error for DFError {}

pub struct DataFrame  {
    pub data: Vec<column::Column >,
}

impl  fmt::Display for DataFrame  {
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

impl  Clone for DataFrame  {
    fn clone(&self) -> Self {
        DataFrame{data: self.data.to_vec()}
    }
}


impl  DataFrame  {
    pub fn new(schema: &str) -> Result<DataFrame> {
        
        //split schema syntax str by
        let column_separators: &[char] = &[',', ';', '\n'][..];
        let token_separators: &[char] = &[':'][..];

        // iterate descriptions of columns, and nested iterate tokens within
        let lcase_schema = schema.to_lowercase();
        let col_descr_iters = lcase_schema
            .split(column_separators)
                    .map(|x| x.rsplit(token_separators))
                    .enumerate();

        //iterator of parsed columns
        let col_parsed_result = col_descr_iters
            .map(|(i_col,mut this_col_dscr_iter)| {
            
                //rsplit yields the last token after : or the only token if no :
                let type_token = this_col_dscr_iter.next().expect("whaat, split cannot yield None");
                let dtype = column::Dtype::from_str_to_res(&type_token[..])?;
                
                //..any other optionally token is the name
                let name_token = this_col_dscr_iter.next().map(String::from);

                //error if more tokens for one column
                if let Some(third_token) = this_col_dscr_iter.next() {
                    Err(column::ColError{
                        errorcode : column::ColErrorcode::SchemaSyntax,
                        error_msg : format!(
                            "unexpected third token \"{}\" for column descr No. {}",
                            third_token, i_col
                        )
                    })?;
                }
            
                Ok(column::Column::new(name_token, dtype))
        });

        //collect parsed results in values and errors
        let (values, errors): (Vec<_>, Vec<_>) = col_parsed_result.partition(|result| result.is_ok());
        
        //downcast'isch Result to error
        let errors: Vec<Box<dyn std::error::Error>> = errors.into_iter().flat_map(Result::err).collect();
        if !errors.is_empty() {
            Err(DFError{
                error_msg: "tentative error complaining about some columns failed schema parsing".to_string(),
                sub_errors: errors,
            })?;
        }

        //if here all good, instanciate data.frame with all good columns
        let values = values.into_iter().flat_map(Result::ok).collect();
        Ok(DataFrame{
            data: values
        })
    }


    pub fn append_line(&mut self, i_line: &str) -> Result<()> {
        
        if i_line.len() == 0 {
            return Ok(());
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
            self.data[i_column].data.push_from_str(cell_str)?;
        }
        Ok(())
    }

 

    pub fn append_str(&mut self, text: &str) -> Result<()> {
        
        let csvstr = lineparser::CsvStr::new(text, 0, 0);
        let mut last_row: usize = 0;
        let mut last_col: usize = 0;
        let crit =  self.data.len() - 1;
        for i in csvstr {
            self.data[i.col].data.push_from_str(i.text)?;

            if i.row != last_row {
                //add to remaining columns if missing cells in line
                if last_col < crit  {
                    for j in (last_col+1)..self.data.len() {
                        self.data[j].data.push_from_str("")?;
                    }
                }
            }
            last_col = i.col;
            last_row = i.row;
        }
        if last_col < crit  {
            for j in (last_col+1)..self.data.len() {
                self.data[j].data.push_from_str("")?;
            }
        }
        Ok(())
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


pub fn csv_read_file_fast (file_name: &str, schema_str: &str) -> Result<DataFrame>  {

    let x = String::from(schema_str);
    let mut df = DataFrame::new(&x)?;


    if let Ok(fbuffer) = FileBuffer::open(&file_name) {
        let lines = str::from_utf8(&fbuffer)?.lines();
        for i_line in lines {       
            df.append_line(&i_line);
        }
    }
    
    Ok(df)
}


pub fn csv_read_file_iter (file_name: &str, schema_str: &str) -> Result<DataFrame>  {

    let x = String::from(schema_str);
    //make empty data frame by schema str
    let mut df = DataFrame::new(&x)?;

    if let Ok(fbuffer) = FileBuffer::open(&file_name) {
        let text =
            str::from_utf8(&fbuffer)
            .expect("not valid UTF-8");


        df.append_str(text)?;
      
    }

    Ok(df)
}

pub fn csv_read_file (file_name: &str, schema_str: &str) -> Result<DataFrame>  {

    let x = String::from(schema_str);
    //make empty data frame by schema str
    let mut df = DataFrame::new(&x)?;


    //if reading succeeded
    if let Ok(lines) = read_lines(file_name) {
        
        for i_line in lines {
            
            // try read a line, skip if empty line
            let i_line = i_line.unwrap();

            df.append_line(&i_line);

        }
    }

    Ok(df)
}

pub fn csv_read_str (csv_str: &str, schema_str: &str) -> Result<DataFrame> {

    let x = String::from(schema_str);
    //make empty data frame by schema str
    let mut df = DataFrame::new(&x)?;

        
    for line in csv_str.lines() {
        df.append_line(line);
    }

    Ok(df)
}

#[allow(dead_code)]
pub fn csv_read_str_iter (csv_str: &str, schema_str: &str) -> Result<DataFrame>  {

    let x = String::from(schema_str);
    //make empty data frame by schema str
    let mut df = DataFrame::new(&x)?;

    df.append_str(csv_str)?;

    Ok(df)
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
    let df = csv_read_str(mycsvstr, myschema).unwrap();

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
        let df = csv_read_str_iter(mycsvstr, myschema).unwrap();

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

    #[test]
    fn test_csv_read_iter_complete_the_lines() {
        //read this csv by this schema 
        let mycsvstr = "1,1.1,3,four\n123\n100,200.200,300,fourhundred\n456\n789\n\n";
        let myschema = "a:intNullable,b:doubleNullable,c:intNullable,someothername:string";
        let df = csv_read_str_iter(mycsvstr, myschema).unwrap();

        println!("iter df looks like ({})",df);
    
        //check ColInt
        let a_ref_col = Box::new(column::ColIntNullable{
            data: vec![Some(1),Some(123),Some(100),Some(456),Some(789),None]
        });
        let a_act_col: &column::ColIntNullable = &df.data[0].data
            .as_any()
            .downcast_ref::<column::ColIntNullable>()
            .expect("not expected column type");
        assert_eq!(
            *a_act_col.data,
            *a_ref_col.data
        );
    
        //check ColDoubleNullablet
        let b_ref_col = Box::new(
            column::ColDoubleNullable{data: vec![Some(1.1),None,Some(200.200),None,None,None]}
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
        macro_rules! vec_of_strings {($($x:expr),*) => (vec![$($x.to_string()),*]);}
        let d_ref_col = Box::new(
            column::ColString{data: vec_of_strings!["four","","fourhundred","","",""]}
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
    fn return_error_if_noncomplete_lines_and_int() {
        //col c is int and must fail
        let mycsvstr = "1,1.1,3,four\n123\n100,200.200,300,fourhundred\n456\n789\n\n";
        let myschema = "a:intNullable,b:doubleNullable,c:int,someothername:string";
        let df = csv_read_str_iter(mycsvstr, myschema);

        let mut test_err_string = String::new();
        if let Err(err) = df {
            test_err_string = err.to_string();
        }
        let ref_error = "".parse::<i32>().unwrap_err();
        
        assert_eq!(test_err_string,ref_error.to_string());


    }

    #[test]
    fn return_error_schema_errors() {
        //col c is int and must fail
       
        let myschema = "a:not_a_col_type,b:third_token:int,,correct_col:string";
        let df = DataFrame::new(myschema);
        
        //see errors
        //df.unwrap();

        let expected_error_msg = "dataframe error tentative error complaining about some columns failed schema parsing
sub error 0: failed to parse \"not_a_col_type\" to any known datatype
sub error 1: failed to parse the token stream \"unexpected third token \"b\" for column descr No. 1\" as a column
sub error 2: failed to parse \"\" to any known datatype".to_string();

        if let Err(err) = df {
            let err_text = err.to_string();
            assert_eq!(expected_error_msg, err_text);
        } else {
            panic!("did not yield any error!!");
        }


    }

}
