//! Minimal Parquet output via the parquet crate's low-level column writers.

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub enum Col {
    Str(Vec<ByteArray>),
    OptStr(Vec<ByteArray>, Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

pub fn write(path: &Path, schema: &str, cols: Vec<Col>) {
    let schema = Arc::new(parse_message_type(schema).expect("schema"));
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .build(),
    );
    let file = File::create(path).expect("create output file");
    let mut w = SerializedFileWriter::new(file, schema, props).expect("parquet writer");
    let mut rg = w.next_row_group().expect("row group");
    let mut cols = cols.into_iter();
    while let Some(mut cw) = rg.next_column().expect("column") {
        match cols.next().expect("column data") {
            Col::Str(v) => {
                cw.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }
            Col::OptStr(v, def) => {
                cw.typed::<ByteArrayType>().write_batch(&v, Some(&def), None).unwrap();
            }
            Col::I32(v) => {
                cw.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            Col::I64(v) => {
                cw.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }
        }
        cw.close().unwrap();
    }
    rg.close().unwrap();
    w.close().unwrap();
}

pub const USER_DAILY: &str = "message user_daily {
  REQUIRED BYTE_ARRAY user_id (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 events;
  REQUIRED INT64 revenue_cents;
  OPTIONAL BYTE_ARRAY top_category (UTF8);
}";

pub const CAMPAIGN_DAILY: &str = "message campaign_daily {
  REQUIRED BYTE_ARRAY campaign_id (UTF8);
  REQUIRED BYTE_ARRAY channel (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 purchasing_sessions;
  REQUIRED INT64 revenue_cents;
  REQUIRED INT64 assisted_sessions;
}";
