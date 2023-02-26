#![feature(test)]

extern crate test;
use test::Bencher;
use polars::prelude::*;
use color_eyre::{Result};


#[bench]
fn bench_xor_1000_ints(b: &mut Bencher) {
    b.iter(|| {
        (0..1000).fold(0, |old, new| old ^ new);
    });
}

#[bench]
fn with_select(b: &mut Bencher) -> Result<()> { 
    let file = std::fs::File::open("yellow_tripdata_2022-01.parquet").unwrap();

    let df = ParquetReader::new( file).finish()?.lazy();
    let agg = df.select([col("VendorID"), col("airport_fee")])
    .groupby(["VendorID"]).agg([col("airport_fee").sum()]).collect()?;


    println!("{:?}", agg);

    Ok(())
}
#[bench]
fn without_select(b: &mut Bencher) -> Result<()> { 
    let file = std::fs::File::open("yellow_tripdata_2022-01.parquet").unwrap();

    let df = ParquetReader::new( file).finish()?.lazy();
    let agg = df.groupby(["VendorID", "payment_type"]).agg([col("airport_fee").sum()]).collect()?;


    println!("{:?}", agg);

    Ok(())
}
