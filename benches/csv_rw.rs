use criterion::{ criterion_group, criterion_main, Criterion};
use teddies::dataframe;

fn csv_str() {
    let mycsvstr = "11,890318,t,0.816093979869038";
    let myschema = "a:int,b:int,c:string,d:double";
    let mut df = dataframe::csv_read_str(mycsvstr, myschema).unwrap();
    let a_csv_line = String::from("11,890318,t,0.816093979869038");
    for _ in 0..100000 {
        df.append_line(&a_csv_line);
    }
}

fn csv_file_fast() {
    let mystring = String::from("myother.csv");
    let myschema = "a:int,b:int,c:string,d:double";
    let _ = dataframe::csv_read_file_fast(&mystring, myschema);
}

fn csv_file_iter() {
    let mystring = String::from("myother.csv");
    let myschema = "a:int,b:int,c:string,d:double";
    let _ = dataframe::csv_read_file_iter(&mystring, myschema);
}

fn csv_file_iter_stringpool() {
    let mystring = String::from("myother.csv");
    let myschema = "a:int,b:int,c:stringpool,d:double";
    let _ = dataframe::csv_read_file_iter(&mystring, myschema);
}


fn polars()  {
    use polars::prelude::*;

    let lf: LazyFrame = LazyCsvReader::new("myother.csv".into())
                        .has_header(false)
                        .finish().unwrap();
   

    lf.collect().unwrap().shape();

}



fn criterion_csv_benchmark(c: &mut Criterion) {
    c.bench_function("csv file iter", |b| b.iter(|| csv_file_iter()));
    c.bench_function("csv file iter stringpool", |b| b.iter(|| csv_file_iter_stringpool()));
    c.bench_function("polars", |b| b.iter(|| polars()));
    //c.bench_function("csv file fast", |b| b.iter(|| csv_file_fast()));
   
    //c.bench_function("csv str", |b| b.iter(|| csv_str()));
}

criterion_group!{
    name = benches;
    // This can be any expression that returns a `Criterion` object.
    config = Criterion::default().sample_size(50);
    targets = criterion_csv_benchmark
}

//criterion_group!(benches, criterion_csv_benchmark);
criterion_main!(benches);