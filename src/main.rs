use datafusion::arrow::array::{Decimal128Array, FixedSizeBinaryArray, LargeStringArray, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;

const TABLE_SIZE: usize = 1_000_000;

fn vec_string() -> Arc<LargeStringArray> {
    let mut rng = thread_rng();

    Arc::new(LargeStringArray::from(
        (0..TABLE_SIZE)
            .map(|_| {
                (0..rng.gen_range(1..=50))
                    .map(|_| rng.sample(Alphanumeric) as char)
                    .collect()
            })
            .collect::<Vec<String>>(),
    ))
}

async fn create_or_load<F, Fut>(ctx: &SessionContext, table_name: &str, generate: F) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<RecordBatch>> + Send + 'static,
{
    let sorted_table_name = format!("{table_name}_sorted");
    let file_name = format!("{table_name}.parquet");
    let sorted_file_name = format!("{sorted_table_name}.parquet");
    if Path::new(&file_name).exists() {
        println!("loading data from {}", &file_name);

        let options = ParquetReadOptions::default();
        ctx.register_parquet(&table_name, &file_name, options).await?;

        let sorted_options = ParquetReadOptions::default().file_sort_order(vec![vec![col("id").sort(true, false)]]);
        ctx.register_parquet(&sorted_table_name, &sorted_file_name, sorted_options).await?;
    } else {
        println!("creating data for {}", table_name);
        let start = Instant::now();
        let batch = generate().await?;

        ctx.register_batch(&table_name, batch)?;
        let df = ctx.sql(&format!("select * from {table_name}")).await?;
        df.write_parquet(&file_name, DataFrameWriteOptions::default(), None).await?;

        let df = ctx.sql(&format!("select * from {table_name} order by id")).await?;
        df.write_parquet(&sorted_file_name, DataFrameWriteOptions::default(), None).await?;
        println!("created data for {} in {:?}", table_name, start.elapsed());
    }
    Ok(())
}

async fn create_simple_fixed() -> Result<RecordBatch> {
    let mut rng = thread_rng();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::FixedSizeBinary(16), false),
            Field::new("name", DataType::LargeUtf8, false),
        ])),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                (0..TABLE_SIZE).map(|_| {
                    let mut bytes = [0; 16];
                    rng.fill(&mut bytes);
                    Some(bytes)
                }),
                16,
            )?),
            vec_string(),
        ],
    )
    .map_err(Into::into)
}

async fn create_simple_string() -> Result<RecordBatch> {
    let mut rng = thread_rng();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::LargeUtf8, false),
            Field::new("name", DataType::LargeUtf8, false),
        ])),
        vec![
            Arc::new(LargeStringArray::from(
                (0..TABLE_SIZE)
                    .map(|_| (0..16).map(|_| rng.sample(Alphanumeric) as char).collect())
                    .collect::<Vec<String>>(),
            )),
            vec_string(),
        ],
    )
    .map_err(Into::into)
}

async fn create_int() -> Result<RecordBatch> {
    let mut rng = thread_rng();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::LargeUtf8, false),
        ])),
        vec![
            Arc::new(UInt64Array::from(
                (0..TABLE_SIZE)
                    .map(|_| rng.gen_range(0..u64::MAX))
                    .collect::<Vec<u64>>(),
            )),
            vec_string(),
        ],
    )
    .map_err(Into::into)
}

// async fn create_int_struct() -> Result<RecordBatch> {
//     let mut rng = thread_rng();
//     let struct_fields = Fields::from_iter([
//         Field::new("left", DataType::UInt64, true),
//         Field::new("right", DataType::UInt64, true),
//     ]);
//
//     let struct_arrays: Vec<Arc<dyn Array>> = vec![
//         Arc::new(UInt64Array::from(
//             (0..TABLE_SIZE)
//                 .map(|_| rng.gen_range(0..u64::MAX))
//                 .collect::<Vec<u64>>(),
//         )),
//         Arc::new(UInt64Array::from(
//             (0..TABLE_SIZE)
//                 .map(|_| rng.gen_range(0..u64::MAX))
//                 .collect::<Vec<u64>>(),
//         )),
//     ];
//
//     RecordBatch::try_new(
//         Arc::new(Schema::new(vec![
//             Field::new("id", DataType::Struct(struct_fields.clone()), false),
//             Field::new("name", DataType::LargeUtf8, false),
//         ])),
//         vec![
//             Arc::new(StructArray::new(struct_fields, struct_arrays, None)),
//             vec_string(),
//         ],
//     )
//     .map_err(Into::into)
// }

async fn create_decimal() -> Result<RecordBatch> {
    let mut rng = thread_rng();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Decimal128(38, 10), false),
            Field::new("name", DataType::LargeUtf8, false),
        ])),
        vec![
            Arc::new(Decimal128Array::from(
                (0..TABLE_SIZE)
                    .map(|_| rng.gen_range(0..i128::MAX))
                    .collect::<Vec<i128>>(),
            )),
            vec_string(),
        ],
    )
    .map_err(Into::into)
}

async fn run(queries: &[String]) -> Result<()> {
    let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "postgres");
    let ctx = SessionContext::new_with_config(config);

    create_or_load(&ctx, "simple_fixed", create_simple_fixed).await?;

    create_or_load(&ctx, "simple_string", create_simple_string).await?;

    create_or_load(&ctx, "int", create_int).await?;
    // create_or_load(&ctx, "int_struct", create_int_struct).await?;
    create_or_load(&ctx, "decimal", create_decimal).await?;

    for sql in queries {
        println!("running:\n```\n{sql}\n```");
        let start = Instant::now();
        let df = ctx.sql(&sql).await?;
        df.show().await?;
        println!("query time: {:?}\n\n", start.elapsed());
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let fwb_needle = "57f16cbaf865bcd9adcc71c03200fd60";
    let fwb_expr = format!("id=arrow_cast(decode('{fwb_needle}', 'hex'), 'FixedSizeBinary(16)')");
    let string_needle = "7wIBWI3Ol4njEVD8";
    let int_needle = "1485542105725837362";
    let decimal_needle = "5714204269946304998258834512.6198419457";
    let decimal_expr = format!("id=arrow_cast('{decimal_needle}', 'Decimal128(38, 10)')");
    let queries = [
        format!("select * from simple_fixed where {fwb_expr}"),
        format!("select * from simple_fixed where {fwb_expr}"),
        format!("select * from simple_fixed_sorted where {fwb_expr}"),
        format!("select * from simple_string where id='{string_needle}'"),
        format!("select * from simple_string where id='{string_needle}'"),
        format!("select * from simple_string_sorted where id='{string_needle}'"),
        format!("select * from int where id={int_needle}"),
        format!("select * from int where id={int_needle}"),
        format!("select * from int_sorted where id={int_needle}"),
        format!("select * from decimal where {decimal_expr}"),
        format!("select * from decimal where {decimal_expr}"),
        format!("select * from decimal_sorted where {decimal_expr}"),
        // "select * from simple_fixed offset 500000 limit 10".to_string(),
        // "select * from simple_string offset 500000 limit 10".to_string(),
        // "select * from int offset 500000 limit 10".to_string(),
        // "select * from decimal offset 500000 limit 10".to_string(),
    ];
    run(&queries).await.unwrap();
}
