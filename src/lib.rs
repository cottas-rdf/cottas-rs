pub fn cottas_rs() -> &'static str {
    "Hello from cottas-rs!"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cottas_rs() {
        assert_eq!(cottas_rs(), "Hello from cottas-rs!");
    }
}
