mod dataframe;

fn main() {

   let mystring = String::from("my.csv");
   let myschema = "a:int,b:doubleNullable,c:int,someothername:string";

   let df = dataframe::csv_read_file(&mystring, myschema);

   println!("Hi I'm \n{}", df);

   let mycsvstr = "1,2.2,3,four\n10,20.20,30,fourty";
   let myschema = "a:int,b:doubleNullable,c:int,someothername:string";
   let mut df2 = dataframe::csv_read_str(mycsvstr, myschema);
   println!("Hi I'm also \n{}", df2);

   let a_csv_line = String::from("42,123.456,112,yep");
   for _ in 0..20 {
      df2.append_line(&a_csv_line);
   }
   println!("Hi I'm also \n{}", df2);

}
