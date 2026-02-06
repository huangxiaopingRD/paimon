/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.spark.commands.{PaimonShowTablePartitionCommand, PaimonShowTablesExtendedCommand}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.command.ShowTablesCommand

case class Spark3ResolutionRules(session: SparkSession)
  extends Rule[LogicalPlan]
  with SQLConfHelper {

  import org.apache.spark.sql.connector.catalog.PaimonCatalogImplicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case ShowTablesCommand(
          ResolvedNamespace(catalog, ns),
          Some(pattern),
          output,
          true,
          Some(tablePartitionSpec)) =>
      val table = Identifier.of(ns.toArray, pattern)
      val resolvedSpec =
        PaimonResolvePartitionSpec.resolve(catalog.asTableCatalog, table, tablePartitionSpec)
      PaimonShowTablePartitionCommand(output, catalog.asTableCatalog, table, resolvedSpec)

    case ShowTablesCommand(ResolvedNamespace(catalog, ns), Some(pattern), output, true, None) =>
      PaimonShowTablesExtendedCommand(catalog.asTableCatalog, ns, pattern, output)
  }
}
