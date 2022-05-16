#[derive(Clone, Debug, PartialEq)]
pub struct CsvStr<'a>{
    pub text: &'a str,
    pub row: usize,
    pub col: usize,
}

impl<'a> CsvStr<'a> {
    pub fn new(text: &'a str, row: usize, col: usize) -> CsvStr {
        CsvStr{
            text,
            row,
            col,
        }
    }
}

impl<'a> Iterator for CsvStr<'a> {
    // We can refer to this type using Self::Item
    type Item = CsvStr<'a>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.text.len() == 0 {
            return None
        }
        //split text into new cell(text of only one cell) and remaing text
        let ((a,b), new_line) = take_cell_str(self.text);
        let new_cell = CsvStr::new(a, self.row, self.col);

        self.text = b;
        self.col += 1;
        if new_line {
            self.col = 0;
            self.row += 1;
        }

        Some(new_cell)
    }
}

impl<'a> FromIterator<CsvStr<'a>> for Vec<Option<CsvStr<'a>>> {
    fn from_iter<I: IntoIterator<Item = CsvStr<'a> >>(iter:I ) -> Self {
        let mut c = Vec::new();

        for i in iter {
                c.push(Some(i));
        }

        c
    }
}

#[allow(dead_code)]
pub fn take_line_str(text: &str) -> (&str, &str) {
    
    let mut chars = text.chars();
    let mut oc = chars.next();
    let mut quote_on: bool = false;
    let mut i_char: usize = 0;
    while let Some(c) = oc {
        match c {
            '\n' => {
                if !quote_on {break;}
            },
            '"' => quote_on = !quote_on,
            _ => ()
        
        }
        i_char += 1;
        oc = chars.next();
    }

    text.split_at(i_char)

}

pub fn take_cell_str(text: &str) -> ((&str, &str), bool) {
    
    let mut chars = text.chars();
    let mut oc = chars.next();
    let mut quote_on: bool = false;
    let mut i_char: usize = 0;
    let mut new_line: bool = false;
    while let Some(c) = oc {
        match c {

            ',' => {
                if !quote_on {break;}
            },
            '\n' => {
                if !quote_on {
                    new_line = true;
                    break;
                }
            },
            '"' => quote_on = !quote_on,
            _ => ()
        
        }
        i_char += 1;
        oc = chars.next();
    }

    let (a,mut b) = text.split_at(i_char);
    
    //if not last cell strip delimter
    if oc.is_some() {
        b = &b[1..];
    }

    ((a,b),new_line)
}

#[cfg(test)]
mod tests {
   use super::*;

    #[test]
    fn test_csv_read_str() {
    //read this csv by this schema 
    let mycsvstr = "1 ,2.2 , 3,four\n10, 20.20,30,fourty";
    let (a, b) = take_line_str(mycsvstr);
    assert_eq!(a, "1 ,2.2 , 3,four");
    assert_eq!(b, "\n10, 20.20,30,fourty");

    }

    #[test]
    fn parse_quoted_str() {
        //read this csv by this schema 
        let mycsvstr = "1 ,\"\n\" , 3,four\n10, 20.20,30,fourty";
        let (a, b) = take_line_str(mycsvstr);
        assert_eq!(a, "1 ,\"\n\" , 3,four");
        assert_eq!(b, "\n10, 20.20,30,fourty");
        assert_ne!(a, "1 ,\"");

    
    }

    #[test]
    fn parse_cell_str() {
        //read this csv by this schema 
        let mycsvstr = "1 ,\"\n\" , 3,four\n10, 20.20,30,fourty";
        let (st, new_line) = take_cell_str(mycsvstr);
        assert_eq!(st.0, "1 ");
        assert_eq!(st.1, "\"\n\" , 3,four\n10, 20.20,30,fourty");
        assert_eq!(new_line, false);

        let mycsvstr = "1 \"\n\" , 3,four\n10, 20.20,30,fourty";
        let (st, new_line) = take_cell_str(mycsvstr);
        assert_eq!(st.0, "1 \"\n\" ");
        assert_eq!(st.1, " 3,four\n10, 20.20,30,fourty");
        assert_eq!(new_line, false);

        let mycsvstr = "fourty\n 34";
        let (st, new_line) = take_cell_str(mycsvstr);
        assert_eq!(st.0, "fourty");
        assert_eq!(st.1, " 34");
        assert_eq!(new_line, true);
    
    }

    #[test]
    fn iter_csv() {
        let mycsvstr = "1 ,\"\n\" , 3,four\n10, 20.20,30,fourty";
        let mut csvstr_obj = CsvStr::new(mycsvstr, 0, 0);
  
        let cells: Vec<CsvStr> = csvstr_obj.into_iter().collect();

        assert_eq!(cells[0],CsvStr::new("1 ",0,0));
        assert_eq!(cells[1],CsvStr::new("\"\n\" ",0,1));
        assert_eq!(cells[2],CsvStr::new(" 3",0,2));
        assert_eq!(cells[3],CsvStr::new("four",0,3));
        assert_eq!(cells[4],CsvStr::new("10",1,0));
        assert_eq!(cells[5],CsvStr::new(" 20.20",1,1));
        assert_eq!(cells[6],CsvStr::new("30",1,2));
        assert_eq!(cells[7],CsvStr::new("fourty",1,3));

    }


}
