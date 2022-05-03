mod dataframe;
pub mod stringpool;

fn main() {

   let mystring = String::from("my.csv");
   let myschema = "a:int,b:doubleNullable,c:int,someothername:string";
   let df = dataframe::csv_read_file(&mystring, myschema);
   let df3 = dataframe::csv_read_file_fast(&mystring, myschema);


   println!("Hi I'm \n{}", df);

   println!("Hi I'm \n{}", df3);

   let mycsvstr = "\"sr\"1,2.2,3,four\n10,20.20,30,fou\"\"rt\"y";
   let myschema = "a:string,b:doubleNullable,c:int,someothername:string";
   let mut df2 = dataframe::csv_read_str(mycsvstr, myschema);
   println!("Hi I'm also \n{}", df2);

   let a_csv_line = String::from("42,123.456,112,yep");
   for _ in 0..10 {
      df2.append_line(&a_csv_line);
   }
   println!("Hi I'm also \n{}", df2);
}
