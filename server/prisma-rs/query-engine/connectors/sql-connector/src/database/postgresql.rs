use crate::{MutationBuilder, PrismaRow, ToPrismaRow, Transaction, Transactional};
use chrono::{DateTime, NaiveDateTime, Utc};
use connector::{error::ConnectorError, ConnectorResult};
use native_tls::TlsConnector;
use postgres::{
    types::ToSql, types::Type as PostgresType, Client, Config, Row as PostgresRow, Transaction as PostgresTransaction,
};
use prisma_common::config::{ConnectionLimit, ConnectionStringConfig, ExplicitConfig, PrismaDatabase};
use prisma_models::{GraphqlId, PrismaValue, ProjectRef, TypeIdentifier};
use prisma_query::{
    ast::{Query, Select},
    visitor::{self, Visitor},
};
use r2d2_postgres::PostgresConnectionManager;
use std::{convert::TryFrom, str::FromStr};
use tokio_postgres::config::SslMode;
use tokio_postgres_native_tls::MakeTlsConnector;

type Pool = r2d2::Pool<PostgresConnectionManager<MakeTlsConnector>>;

pub struct PostgreSql {
    pool: Pool,
}

impl TryFrom<&PrismaDatabase> for PostgreSql {
    type Error = ConnectorError;

    fn try_from(db: &PrismaDatabase) -> ConnectorResult<Self> {
        match db {
            PrismaDatabase::ConnectionString(ref config) => Ok(PostgreSql::try_from(config)?),
            PrismaDatabase::Explicit(ref config) => Ok(PostgreSql::try_from(config)?),
            _ => Err(ConnectorError::DatabaseCreationError(
                "Could not understand the configuration format.",
            )),
        }
    }
}

impl TryFrom<&ExplicitConfig> for PostgreSql {
    type Error = ConnectorError;

    fn try_from(e: &ExplicitConfig) -> ConnectorResult<Self> {
        let mut config = Config::new();
        config.host(&e.host);
        config.port(e.port);
        config.user(&e.user);
        config.ssl_mode(SslMode::Prefer);
        config.dbname("prisma");

        if let Some(ref pw) = e.password {
            config.password(pw);
        }

        Self::new(config, e.limit())
    }
}

impl TryFrom<&ConnectionStringConfig> for PostgreSql {
    type Error = ConnectorError;

    fn try_from(s: &ConnectionStringConfig) -> ConnectorResult<Self> {
        let mut config = Config::from_str(s.uri.as_str())?;
        config.ssl_mode(SslMode::Prefer);
        config.dbname("prisma");

        PostgreSql::new(config, s.limit())
    }
}

impl Transactional for PostgreSql {
    fn with_transaction<F, T>(&self, _: &str, f: F) -> ConnectorResult<T>
    where
        F: FnOnce(&mut Transaction) -> ConnectorResult<T>,
    {
        self.with_client(|client| {
            let mut tx = client.transaction()?;
            let result = f(&mut tx);

            if result.is_ok() {
                tx.commit()?;
            }

            result
        })
    }
}

impl<'a> Transaction for PostgresTransaction<'a> {
    fn write(&mut self, q: Query) -> ConnectorResult<Option<GraphqlId>> {
        let id = match q {
            insert @ Query::Insert(_) => {
                let (sql, params) = dbg!(visitor::Postgres::build(insert));

                let params: Vec<&ToSql> = params.iter().map(|pv| pv as &ToSql).collect();
                let stmt = self.prepare(&sql)?;

                let rows = self.query(&stmt, params.as_slice())?;
                rows.into_iter().rev().next().map(|row| row.get(0))
            }
            query => {
                let (sql, params) = dbg!(visitor::Postgres::build(query));
                let params: Vec<&ToSql> = params.iter().map(|pv| pv as &ToSql).collect();

                let stmt = self.prepare(&sql)?;
                self.execute(&stmt, params.as_slice())?;

                None
            }
        };

        Ok(id)
    }

    fn filter(&mut self, q: Select, idents: &[TypeIdentifier]) -> ConnectorResult<Vec<PrismaRow>> {
        let (sql, params) = dbg!(visitor::Postgres::build(q));
        let params: Vec<&ToSql> = params.iter().map(|pv| pv as &ToSql).collect();

        let stmt = self.prepare(&sql)?;
        let rows = self.query(&stmt, params.as_slice())?;
        let mut result = Vec::new();

        for row in rows {
            result.push(row.to_prisma_row(idents)?);
        }

        Ok(result)
    }

    fn truncate(&mut self, project: ProjectRef) -> ConnectorResult<()> {
        self.write(Query::from("SET CONSTRAINTS ALL DEFERRED"))?;

        for delete in MutationBuilder::truncate_tables(project) {
            self.delete(delete)?;
        }

        Ok(())
    }
}

impl ToPrismaRow for PostgresRow {
    fn to_prisma_row<'b, T>(&'b self, idents: T) -> ConnectorResult<PrismaRow>
    where
        T: IntoIterator<Item = &'b TypeIdentifier>,
    {
        fn convert(row: &PostgresRow, i: usize, typid: &TypeIdentifier) -> ConnectorResult<PrismaValue> {
            let result = match typid {
                TypeIdentifier::String => match row.try_get(i)? {
                    Some(val) => PrismaValue::String(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::GraphQLID | TypeIdentifier::Relation => match row.try_get(i)? {
                    Some(val) => PrismaValue::GraphqlId(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::Float => match row.try_get(i)? {
                    Some(val) => PrismaValue::Float(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::Int => match *row.columns()[i].type_() {
                    PostgresType::INT2 => {
                        let val: i16 = row.try_get(i)?;
                        PrismaValue::Int(val as i64)
                    }
                    PostgresType::INT4 => {
                        let val: i32 = row.try_get(i)?;
                        PrismaValue::Int(val as i64)
                    }
                    _ => PrismaValue::Int(row.try_get(i)?),
                },
                TypeIdentifier::Boolean => match row.try_get(i)? {
                    Some(val) => PrismaValue::Boolean(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::Enum => match row.try_get(i)? {
                    Some(val) => PrismaValue::Enum(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::Json => match row.try_get(i)? {
                    Some(val) => PrismaValue::Json(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::UUID => match row.try_get(i)? {
                    Some(val) => PrismaValue::Uuid(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::DateTime => match row.try_get(i)? {
                    Some(val) => {
                        let ts: NaiveDateTime = val;
                        PrismaValue::DateTime(DateTime::<Utc>::from_utc(ts, Utc))
                    }
                    None => PrismaValue::Null,
                },
            };

            Ok(result)
        }

        let mut row = PrismaRow::default();

        for (i, typid) in idents.into_iter().enumerate() {
            row.values.push(convert(self, i, typid)?);
        }

        Ok(row)
    }
}

impl PostgreSql {
    fn new(config: Config, connections: u32) -> ConnectorResult<PostgreSql> {
        let mut tls_builder = TlsConnector::builder();
        tls_builder.danger_accept_invalid_certs(true); // For Heroku

        let tls = MakeTlsConnector::new(tls_builder.build()?);

        let manager = PostgresConnectionManager::new(config, tls);
        let pool = r2d2::Pool::builder().max_size(connections).build(manager)?;

        Ok(PostgreSql { pool })
    }

    fn with_client<F, T>(&self, f: F) -> ConnectorResult<T>
    where
        F: FnOnce(&mut Client) -> ConnectorResult<T>,
    {
        let mut client = self.pool.get()?;
        let result = f(&mut client);
        result
    }
}
