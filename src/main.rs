use datafusion::arrow::array::{FixedSizeBinaryArray, LargeStringArray, RecordBatch};
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
    let file_name = format!("{}.parquet", table_name);
    if Path::new(&file_name).exists() {
        println!("loading data from {}", &file_name);

        let options = ParquetReadOptions::default().file_sort_order(vec![vec![col("id").sort(true, false)]]);

        ctx.register_parquet(table_name, &file_name, options).await?;
    } else {
        println!("creating data for {}", table_name);
        let start = Instant::now();
        let batch = generate().await?;
        ctx.register_batch(table_name, batch)?;
        // let df = ctx.sql(&format!("select * from {table_name}")).await?;
        let df = ctx.sql(&format!("select * from {table_name} order by id")).await?;
        df.write_parquet(&file_name, DataFrameWriteOptions::default(), None)
            .await?;
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
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    (0..TABLE_SIZE).map(|_| {
                        let mut bytes = [0; 16];
                        rng.fill(&mut bytes);
                        Some(bytes)
                    }),
                    16,
                )?,
            ),
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

async fn run(queries: &[&str]) -> Result<()> {
    let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "postgres");
    let ctx = SessionContext::new_with_config(config);

    create_or_load(&ctx, "simple_fixed", create_simple_fixed).await?;
    create_or_load(&ctx, "simple_string", create_simple_string).await?;

    for sql in queries {
        println!("running:\n```\n{}\n```", sql);
        let start = Instant::now();
        let df = ctx.sql(*sql).await?;
        df.show().await?;
        println!("query time: {:?}\n\n", start.elapsed());
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let queries = [
        r#"select * from simple_fixed
            where id=arrow_cast(decode('7fe764cd931ae0888a34260ac5cb9ff0', 'hex'), 'FixedSizeBinary(16)')"#,
        "select * from simple_string where id='UxmQP5NK3Tb4f9yt'",
        // "select * from simple_fixed offset 500000 limit 10",
        // "select * from simple_string offset 500000 limit 10",
    ];
    run(&queries).await.unwrap();
}
