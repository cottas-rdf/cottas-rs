use rio_turtle::TurtleParser;
use rio_xml::RdfXmlParser;
use std::fs::File;
use std::io::{BufReader, Read};

pub fn parse_rdf_file(
    path: &str,
) -> Result<Vec<(String, String, String, Option<String>)>, Box<dyn std::error::Error>> {
    let format = path.split('.').last().unwrap_or("");
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut quads = Vec::new();

    match format {
        "ttl" | "nt" => {
            let parser = TurtleParser::new(reader, None);
            for triple in parser {
                let t = triple?;
                quads.push((t.subject.to_string(), t.predicate.to_string(), t.object.to_string(), None));
            }
        }
        "rdf" | "xml" => {
            let parser = RdfXmlParser::new(reader);
            for triple in parser {
                let t = triple?;
                quads.push((t.subject.to_string(), t.predicate.to_string(), t.object.to_string(), None));
            }
        }
        _ => return Err(format!("Unsupported RDF format: {}", format).into()),
    }

    Ok(quads)
}
