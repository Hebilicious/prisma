use graphql_parser::{self as gql, query, schema::Document};
use std::env;
use std::fs::File;
use std::io::Read;

pub enum ValidationError {
    EverythingIsBroken,
    Problematic(String),
    Duplicate(String),
}

pub trait Validatable {
    fn validate(&self, doc: &query::Document) -> Result<(), ValidationError>;
}

pub type PrismaSchema = Document;

impl Validatable for PrismaSchema {
    fn validate(&self, doc: &query::Document) -> Result<(), ValidationError> {
        // It's not really ok 😭
        Ok(())
    }
}

pub fn load_schema() -> Result<PrismaSchema, Box<std::error::Error>> {
    let path = env::var("PRISMA_EXAMPLE_SCHEMA").unwrap();
    let mut f = File::open(path).unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();

    Ok(gql::parse_schema(&s).unwrap())
}