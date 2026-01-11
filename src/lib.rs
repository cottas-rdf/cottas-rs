pub mod duckdb;
pub mod export;
pub mod parser;
pub mod utils;

pub use duckdb::{connection_in_memory, has_column, load_into_duckdb, search_in_duckdb, cat_duckdb};
pub use export::{export_to_cottas, write_quads_to_file};
pub use parser::parse_rdf_file;
use std::error::Error;
use std::fs::File;
use ::duckdb::arrow::compute::or_kleene;
pub use utils::extract_format;

pub fn rdf2cottas(
    rdf_file_path: &str,
    cottas_file_path: &str,
    index: &str,
) -> Result<(), Box<dyn Error>> {
    let quads = parse_rdf_file(rdf_file_path)?;
    let quad_mode = quads.iter().any(|q| q.3.is_some());
    let conn = load_into_duckdb(&quads);
    export_to_cottas(&conn, index, cottas_file_path, quad_mode);
    Ok(())
}

pub fn cottas2rdf(cottas_file_path: &str, rdf_file_path: &str) -> Result<(), Box<dyn Error>> {
    let conn = connection_in_memory();
    let has_named_graph = has_column(&conn, cottas_file_path, "g")?;

    let mut file = File::create(rdf_file_path)?;
    write_quads_to_file(&conn, cottas_file_path, has_named_graph, &mut file)?;

    Ok(())
}

pub fn search(
    cottas_file_path: &str,
    triple_pattern: &str,
) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    search_in_duckdb(cottas_file_path, triple_pattern)
}

pub fn cat(
    cottas_file_paths: &[String],
    cottas_cat_file_path: &str,
    index: Option<&str>,
    remove_input_files: Option<bool>
) -> Result<(), Box<dyn Error>> {
    let index = index.unwrap_or("spo");
    let remove_input_files = remove_input_files.unwrap_or(false);

    cat_duckdb(
        cottas_file_paths,
        cottas_cat_file_path,
        index,
        remove_input_files
    )
}

pub fn diff(cottas_file_1_path: &str, cottas_file_2_path: &str, cottas_diff_file_path: &str, index: Option<&str>, remove_input_files: Option<bool>) -> Result<(), Box<dyn Error>> {
    // if index and !is_valid_index(index):
    //     print(f"Index `{index}` is not valid.")
    // return
    //
    // diff_query = f
    // "COPY (SELECT * FROM (SELECT DISTINCT * FROM PARQUET_SCAN('{cottas_file_1_path}') EXCEPT SELECT * FROM PARQUET_SCAN('{cottas_file_2_path}'))"
    // if index:
    //     diff_query += " ORDER BY "
    // for p in index:
    //     diff_query += f
    // "{p}, "
    // diff_query = diff_query
    // [: -2]
    // diff_query += f
    // ") TO '{cottas_diff_file_path}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22, PARQUET_VERSION v2, KV_METADATA {{index: '{index.lower()}'}})"
    // duckdb.execute(diff_query)
    //
    // if remove_input_files:
    //     os.remove(cottas_file_1_path)
    //     os.remove(cottas_file_2_path)
    Ok(())
}

