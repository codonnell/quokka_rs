#![feature(is_sorted)]

mod table_provider;

use datafusion_sql::parser;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let result = parser::DFParser::parse_sql_with_dialect(
        "create table t (id integer primary key, v text)",
        &sqlparser::dialect::GenericDialect {},
    );
    println!("Result: {:#?}", result);
    Ok(())
}
