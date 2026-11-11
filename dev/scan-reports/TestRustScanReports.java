/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReportParser;
import org.junit.jupiter.api.Test;

class TestRustScanReports {
  @Test
  void captureReports() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Path root = Path.of(System.getenv("SCAN_REPORT_OUTPUT"));
    JsonNode cases = mapper.readTree(root.resolve("rust.json").toFile());
    ArrayNode results = mapper.createArrayNode();
    for (JsonNode fixture : cases) {
      ObjectNode result = results.addObject();
      String name = fixture.get("fixture").asText();
      result.put("fixture", name);
      List<ScanReport> reports = new ArrayList<>();
      try (HadoopFileIO io = new HadoopFileIO()) {
        Table table = new BaseTable(
            new StaticTableOperations(fixture.get("metadata").asText(), io),
            fixture.get("table_ident").asText(),
            report -> reports.add((ScanReport) report));
        TableScan scan = table.newScan();
        if (name.equals("time-travel")) {
          scan = scan.useSnapshot(table.currentSnapshot().snapshotId());
        }
        if (!fixture.get("threshold").isNull()) {
          scan = scan.filter(Expressions.lessThan(fixture.get("filter_column").asText(), fixture.get("threshold").asLong()));
        }
        if (name.equals("all-pruned")) { scan = scan.filter(Expressions.equal("x", 200L)); }
        if (name.equals("projection")) { scan = scan.caseSensitive(false).select("X", "Z"); }
        if (name.equals("metadata-projection")) {
          // select() resolves table columns only; project() can include metadata columns.
          scan = scan.project(new Schema(table.schema().schemaId(), List.of(
              table.schema().findField("x"), MetadataColumns.FILE_PATH,
              MetadataColumns.ROW_POSITION, MetadataColumns.SPEC_ID)));
        }
        result.set("raw_filter", mapper.readTree(org.apache.iceberg.expressions.ExpressionParser.toJson(scan.filter())));
        ArrayNode plans = result.putArray("plans");
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles();
             CloseableIterator<FileScanTask> iterator = tasks.iterator()) {
          int consume = fixture.path("consume").isNull() ? -1 : fixture.get("consume").asInt();
          while (consume < 0 ? iterator.hasNext() : plans.size() < consume) {
            FileScanTask task = iterator.next();
            ObjectNode plan = plans.addObject();
            plan.put("path", task.file().location());
            plan.put("size", task.file().fileSizeInBytes());
            ArrayNode deletes = plan.putArray("deletes");
            for (DeleteFile delete : task.deletes()) {
              ObjectNode d = deletes.addObject();
              d.put("path", delete.location());
              d.put("size", delete.fileSizeInBytes());
              d.put("content", delete.content().id());
              if (delete.contentSizeInBytes() == null) { d.putNull("content_size"); }
              else { d.put("content_size", delete.contentSizeInBytes()); }
              if (delete.contentOffset() == null) { d.putNull("content_offset"); }
              else { d.put("content_offset", delete.contentOffset()); }
            }
          }
        }
        catch (Exception e) {
          result.put("planning_error", e.toString());
        }
        if (reports.size() != 1) { throw new IllegalStateException("Reports: " + reports.size()); }
        result.set("report", mapper.readTree(ScanReportParser.toJson(reports.get(0))));
      } catch (Exception e) {
        result.put("error", e.toString());
      }
    }
    Files.writeString(root.resolve("java.json"), mapper.writerWithDefaultPrettyPrinter().writeValueAsString(results));
  }
}
