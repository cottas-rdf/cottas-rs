use cottas_rs::*;
use polars::polars_utils::parma::raw::Key;
use polars::prelude::*;
use std::path::Path;

#[test]
fn test_rdf2cottas() {
    let source_file = "tests/data/example.ttl";
    let target_file = "tests/data/output.cottas";
    let index = "spo";

    rdf2cottas(source_file, target_file, index).unwrap();

    // Check that target file exists
    assert!(Path::new(target_file).exists());

    let file = std::fs::File::open(target_file).unwrap();
    let df = ParquetReader::new(file).finish().unwrap();

    assert!(df.height() > 0, "The file .cottas is empty");
    println!("{:?}", df.head(Some(5)));
}

#[test]
fn test_cottas2rdf() {
    let cottas_file = "tests/data/example.cottas";
    let rdf_file = "tests/data/output.rdf";

    cottas2rdf(cottas_file, rdf_file).unwrap();

    assert!(Path::new(rdf_file).exists());

    let content = std::fs::read_to_string(rdf_file).unwrap();
    println!(
        "{}",
        &content.lines().take(5).collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn test_search_all_triples() {
    let cottas_file = "tests/data/example.cottas";
    let pattern = "?s ?p ?o";

    let results = search(cottas_file, pattern).unwrap();

    println!("Found {} triples:", results.len());
    for (i, row) in results.iter().enumerate() {
        println!("  {}: {} {} {}", i + 1, row[0], row[1], row[2]);
    }

    assert_eq!(results.len(), 3, "Expected 3 triples");

    for row in &results {
        assert_eq!(row.len(), 3, "Each row should have 3 elements");
    }
}
