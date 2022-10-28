use arrow::datatypes::SchemaRef;
use clap::Parser;
use arrow::csv;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;


use std::fs::File;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
   // Input CSV
   #[arg(short, long)]
   csv: String,

   // Output Parquet
   #[arg(short, long)]
   parquet: String,
}


fn create_csv_reader(filename: String) -> csv::Reader<File> {
    let file = File::open(filename).unwrap();
    let builder = csv::ReaderBuilder::new().has_header(true);
    let reader = builder.build(file).unwrap();
    reader
}

fn create_parquet_writer(filename: String, schema : SchemaRef) -> ArrowWriter<File> {
    let file = File::create(filename).unwrap();
    let props = WriterProperties::builder().set_compression(parquet::basic::Compression::SNAPPY).build();
    let writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer

}

fn main() {
   let args = Args::parse();

   let mut csv_reader = create_csv_reader(args.csv);
   let schema = csv_reader.schema();
   let mut parquet_writer = create_parquet_writer(args.parquet, schema);
   while let Some(batch) = csv_reader.next() {
       parquet_writer.write(&batch.unwrap()).unwrap();
   }
   let _ = parquet_writer.close().unwrap();
}
