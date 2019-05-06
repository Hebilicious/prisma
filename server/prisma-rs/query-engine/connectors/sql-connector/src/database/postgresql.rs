use connector::ConnectorResult;
use native_tls::TlsConnector;
use postgres::Config;
use prisma_common::config::{ConnectionLimit, ConnectionStringConfig, ExplicitConfig};
use r2d2_postgres::PostgresConnectionManager;
use std::str::FromStr;
use tokio_postgres::config::SslMode;
use tokio_postgres_native_tls::MakeTlsConnector;

type Pool = r2d2::Pool<PostgresConnectionManager<MakeTlsConnector>>;

pub struct PostgreSql {
    pool: Pool,
}

impl PostgreSql {
    pub fn from_explicit(prisma_config: &ExplicitConfig) -> ConnectorResult<PostgreSql> {
        let mut config = Config::new();
        config.host(&prisma_config.host);
        config.port(prisma_config.port);
        config.user(&prisma_config.user);
        config.ssl_mode(SslMode::Prefer);

        if let Some(ref pw) = prisma_config.password {
            config.password(pw);
        }

        PostgreSql::new(config, prisma_config.limit())
    }

    pub fn from_connection_str(prisma_config: &ConnectionStringConfig) -> ConnectorResult<PostgreSql> {
        let mut config = Config::from_str(prisma_config.uri.as_str())?;
        config.ssl_mode(SslMode::Prefer);

        PostgreSql::new(config, prisma_config.limit())
    }

    fn new(config: Config, connections: u32) -> ConnectorResult<PostgreSql> {
        let mut tls_builder = TlsConnector::builder();
        tls_builder.danger_accept_invalid_certs(true); // For Heroku

        let tls = MakeTlsConnector::new(tls_builder.build()?);

        let manager = PostgresConnectionManager::new(config, tls);
        let pool = r2d2::Pool::builder().max_size(connections).build(manager)?;

        Ok(PostgreSql { pool })
    }
}
