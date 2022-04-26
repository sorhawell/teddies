use criterion::{black_box, criterion_group, criterion_main, Criterion};
use teddies::dataframe;

fn csv_str() {
    let mycsvstr = "1,2.2,3,four\n10,20.20,30,fourty";
    let myschema = "a:int,b:doubleNullable,c:int,someothername:string";
    let mut df = dataframe::csv_read_str(mycsvstr, myschema);
    let a_csv_line = String::from("42,123.456,112,yep");
    for _ in 0..1000000 {
        df.append_line(&a_csv_line);
    }
}

fn csv_file() {
    let mystring = String::from("myother.csv");
    let myschema = "a:int,b:int,c:string,d:double";
    let df = dataframe::csv_read_file(&mystring, myschema);
}


fn criterion_csv_benchmark(c: &mut Criterion) {
    c.bench_function("csv file", |b| b.iter(|| csv_file()));
    c.bench_function("csv str", |b| b.iter(|| csv_str()));
}

criterion_group!{
    name = benches;
    // This can be any expression that returns a `Criterion` object.
    config = Criterion::default().sample_size(10);
    targets = criterion_csv_benchmark
}

//criterion_group!(benches, criterion_csv_benchmark);
criterion_main!(benches);