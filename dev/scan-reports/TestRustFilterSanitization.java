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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.expressions.*;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestRustFilterSanitization {
  static Type type(String name) {
    if (name.equals("timestamp_ns")) return Types.TimestampNanoType.withoutZone();
    if (name.equals("timestamptz_ns")) return Types.TimestampNanoType.withZone();
    return Types.fromPrimitiveString(name);
  }

  static Literal<?> literal(Type type, JsonNode value) {
    Literal<?> raw = switch (type.typeId()) {
      case BOOLEAN -> Literal.of(value.asBoolean());
      case INTEGER, DATE -> Literal.of(value.intValue());
      case LONG, TIME, TIMESTAMP -> Literal.of(value.longValue());
      // The long-to-nano conversion interprets longs as micros; use an ISO value for typed nanos.
      case TIMESTAMP_NANO -> Literal.of(type.equals(Types.TimestampNanoType.withZone())
          ? org.apache.iceberg.util.DateTimeUtil.nanosToIsoTimestamptz(value.longValue())
          : org.apache.iceberg.util.DateTimeUtil.nanosToIsoTimestamp(value.longValue()));
      case FLOAT -> Literal.of(Float.valueOf(value.asText()));
      case DOUBLE -> Literal.of(Double.valueOf(value.asText()));
      case DECIMAL -> Literal.of(new BigDecimal(value.asText()));
      case UUID -> Literal.of(UUID.fromString(value.asText()));
      case BINARY, FIXED -> Literal.of(ByteBuffer.wrap(HexFormat.of().parseHex(value.asText())));
      default -> Literal.of(value.asText());
    };
    return raw.to(type);
  }

  @SuppressWarnings("unchecked")
  @Test
  void capture() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode document = (ObjectNode) mapper.readTree(Path.of(System.getenv("FILTER_CASES")).toFile());
    long now = document.get("now_micros").asLong();
    // Only the clock is fixed. Binding and visits execute the pinned Java production code.
    Class<?> sanitizerClass = Class.forName("org.apache.iceberg.expressions.ExpressionUtil$ExpressionSanitizer");
    Constructor<?> ctor = sanitizerClass.getDeclaredConstructor();
    ctor.setAccessible(true);
    Field nowField = sanitizerClass.getDeclaredField("now");
    Field todayField = sanitizerClass.getDeclaredField("today");
    nowField.setAccessible(true);
    todayField.setAccessible(true);
    for (JsonNode item : document.get("cases")) {
      Object sanitizer = ctor.newInstance();
      nowField.setLong(sanitizer, now);
      todayField.setInt(sanitizer, (int) Math.floorDiv(now, 86_400_000_000L));
      Type inputType = type(item.get("type").asText());
      Type fieldType = type(item.path("field_type").asText(item.get("type").asText()));
      Schema schema = new Schema(Types.NestedField.optional(1, "value", fieldType));
      String name = item.path("reference").asText("value");
      String opName = item.path("op").asText("eq");
      Expression expression;
      if (opName.equals("compound")) {
        expression = Expressions.or(Expressions.and(Expressions.lessThan(name, item.get("lower").asInt()),
            Expressions.not(Expressions.greaterThanOrEqual(name, item.get("upper").asInt()))), Expressions.isNull(name));
      } else {
        Expression.Operation op = Expression.Operation.fromString(opName.replace('-', '_'));
        if (opName.equals("in") || opName.equals("not-in")) {
          List<Object> values = new ArrayList<>();
          for (JsonNode value : item.get("value")) values.add(literal(inputType, value));
          expression = Expressions.predicate(op, name, values);
        } else if (item.get("value").isNull()) {
          expression = Expressions.predicate(op, name);
        } else {
          expression = Expressions.predicate(op, name, literal(inputType, item.get("value")));
        }
      }
      Expression bound = Binder.bind(schema.asStruct(), expression, item.path("case_sensitive").asBoolean(true));
      Expression result;
      try {
        result = ExpressionVisitors.visit(bound, (ExpressionVisitors.ExpressionVisitor<Expression>) sanitizer);
        ((ObjectNode) item).remove("binding_fallback");
      } catch (RuntimeException e) {
        // ExpressionUtil.sanitize(struct, expression, caseSensitive) has the same fallback.
        result = ExpressionVisitors.visit(expression, (ExpressionVisitors.ExpressionVisitor<Expression>) sanitizer);
        ((ObjectNode) item).put("binding_fallback", e.getClass().getSimpleName());
      }
      ((ObjectNode) item).set("expected", mapper.readTree(ExpressionParser.toJson(result)));
    }
    mapper.writerWithDefaultPrettyPrinter().writeValue(Path.of(System.getenv("FILTER_OUTPUT")).toFile(), document);
  }
}
