use crate::{data_model, PrismaResult};
use core::ReadQueryExecutor;
use prisma_common::config::{self, ConnectionLimit, PrismaConfig, PrismaDatabase};
use prisma_models::SchemaRef;
use std::{convert::TryFrom, sync::Arc};

#[cfg(feature = "sql")]
use sql_connector::{database::PostgreSql, database::SqlDatabase, database::Sqlite};

#[derive(DebugStub)]
pub struct PrismaContext {
    pub config: PrismaConfig,
    pub schema: SchemaRef,

    #[debug_stub = "#QueryExecutor#"]
    pub read_query_executor: ReadQueryExecutor,
}

impl PrismaContext {
    pub fn new() -> PrismaResult<Self> {
        let prisma_config = config::load().unwrap();

        let db_name = prisma_config
            .databases
            .get("default")
            .unwrap()
            .db_name()
            .expect("database was not set");

        match prisma_config.databases.get("default") {
            Some(PrismaDatabase::File(ref config)) if config.connector == "sqlite-native" => {
                let db_name = config.db_name();
                let db_folder = config
                    .database_file
                    .trim_end_matches(&format!("{}.db", db_name))
                    .trim_end_matches("/");

                let sqlite = Sqlite::new(db_folder.to_owned(), config.limit(), false).unwrap();
                let data_resolver = SqlDatabase::new(sqlite);

                let read_query_executor: ReadQueryExecutor = ReadQueryExecutor {
                    data_resolver: Arc::new(data_resolver),
                };

                Ok(Self {
                    config: prisma_config,
                    schema: data_model::load(db_name)?,
                    read_query_executor,
                })
            }
            Some(database) => match database.connector() {
                "postgres-native" => {
                    let postgres = PostgreSql::try_from(database).unwrap();
                    let data_resolver = SqlDatabase::new(postgres);

                    let read_query_executor: ReadQueryExecutor = ReadQueryExecutor {
                        data_resolver: Arc::new(data_resolver),
                    };

                    Ok(Self {
                        config: prisma_config,
                        schema: data_model::load(db_name)?,
                        read_query_executor,
                    })
                }
                connector => panic!("Unsupported connector {}", connector),
            },
            None => panic!("Couldn't find default database"),
        }
    }
}
