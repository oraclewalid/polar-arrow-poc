#![feature(test)]

extern crate test;
use test::Bencher;
use color_eyre::{Result};
use polars::prelude::*;
use reqwest::blocking::Client;
use std::io::Cursor;

fn main() -> Result<()> { 
    let mut file = std::fs::File::open("yellow_tripdata_2022-01.parquet").unwrap();

    let df = ParquetReader::new(&mut file).finish()?.lazy();
    let agg = df.select([col("VendorID"), col("airport_fee")]).groupby(["VendorID"]).agg([col("airport_fee").sum()]).collect()?;


    println!("{:?}", agg);

    Ok(())
}

#[bench]
fn with_select(b: &mut Bencher) -> Result<()> { 
    b.iter(|| {
        let file = std::fs::File::open("yellow_tripdata_2022-01.parquet").unwrap();

        let df = ParquetReader::new( file).finish()?.lazy();
        let agg = df.select([col("VendorID"), col("airport_fee")])
        .groupby(["VendorID"]).agg([col("airport_fee").sum()]).collect();

        println!("{:?}", agg);
        Ok::<&str, PolarsError>("")
    });
    

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
