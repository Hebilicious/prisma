use crate::{DomainError, DomainResult};
use chrono::{DateTime, Utc};
use graphql_parser::query::Value as GraphqlValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{convert::TryFrom, fmt};
use uuid::Uuid;

#[cfg(feature = "sql")]
use prisma_query::ast::*;

#[cfg(feature = "sqlite")]
use rusqlite::types::{FromSql as FromSqlite, FromSqlResult, ValueRef};

#[cfg(feature = "postgresql")]
use postgres::types::{FromSql as FromPostgreSql, Type as PType};

pub type PrismaListValue = Option<Vec<PrismaValue>>;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
pub enum GraphqlId {
    String(String),
    Int(usize),
    UUID(Uuid),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum PrismaValue {
    String(String),
    Float(f64),
    Boolean(bool),
    DateTime(DateTime<Utc>),
    Enum(String),
    Json(Value),
    Int(i64),
    Relation(usize),
    Null,
    Uuid(Uuid),
    GraphqlId(GraphqlId),
    List(PrismaListValue),
}

impl PrismaValue {
    pub fn is_null(&self) -> bool {
        match self {
            PrismaValue::Null => true,
            _ => false,
        }
    }

    pub fn from_value(v: &GraphqlValue) -> Self {
        match v {
            GraphqlValue::Boolean(b) => PrismaValue::Boolean(b.clone()),
            GraphqlValue::Enum(e) => PrismaValue::Enum(e.clone()),
            GraphqlValue::Float(f) => PrismaValue::Float(f.clone()),
            GraphqlValue::Int(i) => PrismaValue::Int(i.as_i64().unwrap()),
            GraphqlValue::Null => PrismaValue::Null,
            GraphqlValue::String(s) => PrismaValue::String(s.clone()),
            GraphqlValue::List(l) => PrismaValue::List(Some(l.iter().map(|i| Self::from_value(i)).collect())),
            _ => unimplemented!(),
        }
    }
}

impl fmt::Display for PrismaValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PrismaValue::String(x) => x.fmt(f),
            PrismaValue::Float(x) => x.fmt(f),
            PrismaValue::Boolean(x) => x.fmt(f),
            PrismaValue::DateTime(x) => x.fmt(f),
            PrismaValue::Enum(x) => x.fmt(f),
            PrismaValue::Json(x) => x.fmt(f),
            PrismaValue::Int(x) => x.fmt(f),
            PrismaValue::Relation(x) => x.fmt(f),
            PrismaValue::Null => "null".fmt(f),
            PrismaValue::Uuid(x) => x.fmt(f),
            PrismaValue::GraphqlId(x) => match x {
                GraphqlId::String(x) => x.fmt(f),
                GraphqlId::Int(x) => x.fmt(f),
                GraphqlId::UUID(x) => x.fmt(f),
            },
            PrismaValue::List(x) => {
                let as_string = format!("{:?}", x);
                as_string.fmt(f)
            }
        }
    }
}

impl From<&str> for PrismaValue {
    fn from(s: &str) -> Self {
        PrismaValue::from(s.to_string())
    }
}

impl From<String> for PrismaValue {
    fn from(s: String) -> Self {
        PrismaValue::String(s)
    }
}

impl From<f64> for PrismaValue {
    fn from(s: f64) -> Self {
        PrismaValue::Float(s)
    }
}

impl From<f32> for PrismaValue {
    fn from(s: f32) -> Self {
        PrismaValue::Float(s as f64)
    }
}

impl From<bool> for PrismaValue {
    fn from(s: bool) -> Self {
        PrismaValue::Boolean(s)
    }
}

impl From<i32> for PrismaValue {
    fn from(s: i32) -> Self {
        PrismaValue::Int(s as i64)
    }
}

impl From<i64> for PrismaValue {
    fn from(s: i64) -> Self {
        PrismaValue::Int(s)
    }
}

impl From<Uuid> for PrismaValue {
    fn from(s: Uuid) -> Self {
        PrismaValue::Uuid(s)
    }
}

impl From<PrismaListValue> for PrismaValue {
    fn from(s: PrismaListValue) -> Self {
        PrismaValue::List(s)
    }
}

impl From<GraphqlId> for PrismaValue {
    fn from(id: GraphqlId) -> PrismaValue {
        PrismaValue::GraphqlId(id)
    }
}

impl From<&GraphqlId> for PrismaValue {
    fn from(id: &GraphqlId) -> PrismaValue {
        PrismaValue::GraphqlId(id.clone())
    }
}

impl TryFrom<PrismaValue> for PrismaListValue {
    type Error = DomainError;

    fn try_from(s: PrismaValue) -> DomainResult<PrismaListValue> {
        match s {
            PrismaValue::List(l) => Ok(l),
            PrismaValue::Null => Ok(None),
            _ => Err(DomainError::ConversionFailure("PrismaValue", "PrismaListValue")),
        }
    }
}

impl TryFrom<PrismaValue> for GraphqlId {
    type Error = DomainError;

    fn try_from(value: PrismaValue) -> DomainResult<GraphqlId> {
        match value {
            PrismaValue::GraphqlId(id) => Ok(id),
            _ => Err(DomainError::ConversionFailure("PrismaValue", "GraphqlId")),
        }
    }
}

impl TryFrom<PrismaValue> for i64 {
    type Error = DomainError;

    fn try_from(value: PrismaValue) -> DomainResult<i64> {
        match value {
            PrismaValue::Int(i) => Ok(i),
            _ => Err(DomainError::ConversionFailure("PrismaValue", "i64")),
        }
    }
}

#[cfg(feature = "sql")]
impl From<GraphqlId> for DatabaseValue {
    fn from(id: GraphqlId) -> DatabaseValue {
        match id {
            GraphqlId::String(s) => s.into(),
            GraphqlId::Int(i) => (i as i64).into(),
            GraphqlId::UUID(u) => u.into(),
        }
    }
}

#[cfg(feature = "sql")]
impl From<&GraphqlId> for DatabaseValue {
    fn from(id: &GraphqlId) -> DatabaseValue {
        id.clone().into()
    }
}

#[cfg(feature = "sql")]
impl From<PrismaValue> for DatabaseValue {
    fn from(pv: PrismaValue) -> DatabaseValue {
        match pv {
            PrismaValue::String(s) => s.into(),
            PrismaValue::Float(f) => (f as f64).into(),
            PrismaValue::Boolean(b) => b.into(),
            PrismaValue::DateTime(d) => d.into(),
            PrismaValue::Enum(e) => e.into(),
            PrismaValue::Json(j) => j.to_string().into(),
            PrismaValue::Int(i) => (i as i64).into(),
            PrismaValue::Relation(i) => (i as i64).into(),
            PrismaValue::Null => DatabaseValue::Parameterized(ParameterizedValue::Null),
            PrismaValue::Uuid(u) => u.into(),
            PrismaValue::GraphqlId(id) => id.into(),
            PrismaValue::List(_) => panic!("List values are not supported here"),
        }
    }
}

#[cfg(feature = "sqlite")]
impl FromSqlite for GraphqlId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value
            .as_str()
            .and_then(|strval| {
                let res = Uuid::from_slice(strval.as_bytes())
                    .map(|uuid| GraphqlId::UUID(uuid))
                    .unwrap_or_else(|_| GraphqlId::String(strval.to_string()));

                Ok(res)
            })
            .or_else(|_| value.as_i64().map(|intval| GraphqlId::Int(intval as usize)))
    }
}

#[cfg(feature = "postgresql")]
impl<'a> FromPostgreSql<'a> for GraphqlId {
    fn from_sql(ty: &PType, raw: &'a [u8]) -> Result<GraphqlId, Box<dyn std::error::Error + Sync + Send>> {
        let res = match *ty {
            PType::INT2 => GraphqlId::Int(i16::from_sql(ty, raw)? as usize),
            PType::INT4 => GraphqlId::Int(i32::from_sql(ty, raw)? as usize),
            PType::INT8 => GraphqlId::Int(i64::from_sql(ty, raw)? as usize),
            PType::UUID => GraphqlId::UUID(Uuid::from_sql(ty, raw)?),
            _ => GraphqlId::String(String::from_sql(ty, raw)?),
        };

        Ok(res)
    }

    fn accepts(ty: &PType) -> bool {
        <&str as FromPostgreSql>::accepts(ty)
            || <Uuid as FromPostgreSql>::accepts(ty)
            || <i16 as FromPostgreSql>::accepts(ty)
            || <i32 as FromPostgreSql>::accepts(ty)
            || <i64 as FromPostgreSql>::accepts(ty)
    }
}

impl From<&str> for GraphqlId {
    fn from(s: &str) -> Self {
        GraphqlId::from(s.to_string())
    }
}

impl From<String> for GraphqlId {
    fn from(s: String) -> Self {
        GraphqlId::String(s)
    }
}

impl From<usize> for GraphqlId {
    fn from(id: usize) -> Self {
        GraphqlId::Int(id)
    }
}

impl From<Uuid> for GraphqlId {
    fn from(uuid: Uuid) -> Self {
        GraphqlId::UUID(uuid)
    }
}
