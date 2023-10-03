/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.util

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaErrors

import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.AnalysisErrorAt
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors

trait AnalysisHelper {
  import AnalysisHelper._

  // Keeping the following two methods for backward compatibility with previous Delta versions.
  protected def tryResolveReferences(
      sparkSession: SparkSession)(
      expr: Expression,
      planContainingExpr: LogicalPlan): Expression =
  tryResolveReferencesForExpressions(sparkSession)(Seq(expr), planContainingExpr.children).head

  protected def tryResolveReferencesForExpressions(
      sparkSession: SparkSession,
      exprs: Seq[Expression],
      planContainingExpr: LogicalPlan): Seq[Expression] =
  tryResolveReferencesForExpressions(sparkSession)(exprs, planContainingExpr.children)

  /**
   * Resolve expressions using the attributes provided by `planProvidingAttrs`. Throw an error if
   * failing to resolve any expressions.
   */
  protected def resolveReferencesForExpressions(
      sparkSession: SparkSession,
      exprs: Seq[Expression],
      planProvidingAttrs: LogicalPlan): Seq[Expression] = {
    val resolvedExprs =
      tryResolveReferencesForExpressions(sparkSession)(exprs, Seq(planProvidingAttrs))
    resolvedExprs.foreach { expr =>
      if (!expr.resolved) {
        throw new AnalysisException(
          s"cannot resolve ${expr.sql} given $planProvidingAttrs")
      }
    }
    resolvedExprs
  }

  /**
   * Resolve expressions using the attributes provided by `planProvidingAttrs`, ignoring errors.
   */
  protected def tryResolveReferencesForExpressions(
      sparkSession: SparkSession)(
      exprs: Seq[Expression],
      plansProvidingAttrs: Seq[LogicalPlan]): Seq[Expression] = {
    val newPlan = FakeLogicalPlan(exprs, plansProvidingAttrs)
    sparkSession.sessionState.analyzer.execute(newPlan) match {
      case FakeLogicalPlan(resolvedExprs, _) =>
        // Return even if it did not successfully resolve
        resolvedExprs
      case _ =>
        // This is unexpected
        throw DeltaErrors.analysisException(
          s"Could not resolve expression $exprs", plan = Some(newPlan))
    }
  }

  protected def toDataset(sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[Row] = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  protected def improveUnsupportedOpError(f: => Unit): Unit = {
    val possibleErrorMsgs = Seq(
      "is only supported with v2 table", // full error: DELETE is only supported with v2 tables
      "is not supported temporarily",    // full error: UPDATE TABLE is not supported temporarily
      "Table does not support read",
      "Table implementation does not support writes"
    ).map(_.toLowerCase())

    def isExtensionOrCatalogError(error: Exception): Boolean = {
      possibleErrorMsgs.exists { m =>
        error.getMessage != null && error.getMessage.toLowerCase().contains(m)
      }
    }

    try { f } catch {
      case e: Exception if isExtensionOrCatalogError(e) =>
        throw DeltaErrors.configureSparkSessionWithExtensionAndCatalog(Some(e))
    }
  }

}

object AnalysisHelper {
  /** LogicalPlan to help resolve the given expression */
  case class FakeLogicalPlan(
      exprs: Seq[Expression],
      children: Seq[LogicalPlan])
    extends LogicalPlan
  {
    override def output: Seq[Attribute] = Nil

    override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): FakeLogicalPlan = copy(children = newChildren)
  }

  /**
   *  Parses and returns a catalog-aware table identifier (e.g. catalog.database.table).
   *
   *  Delta table identifiers need to be catalog-aware, but the Spark parser's parseTableIdentifier
   *  method only handles two-part identifiers (e.g. database.table), and its
   *  parseMultipartIdentifier method returns a Seq[String] that is not easily converted to a
   *  [[TableIdentifier]] (because the converters in [[CatalogV2Implicits]] are not public).
   */
  def parseTableIdentifier(spark: SparkSession, tableName: String): TableIdentifier = {
    spark.sessionState.sqlParser.parseMultipartIdentifier(tableName) match {
      case Seq(table) => TableIdentifier(table)
      case Seq(db, table) => TableIdentifier(table, Some(db))
      case Seq(catalog, db, table) => TableIdentifier(table, Some(db), Some(catalog))
      case other => throw QueryCompilationErrors.identifierTooManyNamePartsError(tableName)
    }
  }

  def getTableCatalog(spark: SparkSession): TableCatalog = {
    spark.sessionState.catalogManager.currentCatalog match {
      case tableCatalog: TableCatalog => tableCatalog
      case otherCatalog =>
        throw new IllegalArgumentException(s"Catalog ${otherCatalog.name} does not support tables")
    }
  }
}
