package com.prisma.api.connector.native

import java.sql.Driver

import com.prisma.api.connector.postgres.PostgresApiConnector
import com.prisma.api.connector.sqlite.SQLiteApiConnector
import com.prisma.api.connector.{ApiConnector, DataResolver, DatabaseMutactionExecutor}
import com.prisma.config.DatabaseConfig
import com.prisma.shared.models.{ConnectorCapabilities, Project, ProjectIdEncoder}

import scala.concurrent.{ExecutionContext, Future}

case class ApiConnectorNative(config: DatabaseConfig)(implicit ec: ExecutionContext) extends ApiConnector {
  lazy val base =
    SQLiteApiConnector(config, new org.sqlite.JDBC)

  override def initialize(): Future[Unit] = Future.unit
  override def shutdown(): Future[Unit] = Future.unit

  override def databaseMutactionExecutor: DatabaseMutactionExecutor =
    NativeDatabaseMutactionExecutor(base.databaseMutactionExecutor.slickDatabase)

  override def dataResolver(project: Project): DataResolver       = NativeDataResolver(project)
  override def masterDataResolver(project: Project): DataResolver = NativeDataResolver(project)
  override def projectIdEncoder: ProjectIdEncoder                 = ProjectIdEncoder('_')

  override val capabilities = ConnectorCapabilities.sqliteNative
}
