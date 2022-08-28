/*
 * Copyright 2022 Israel Herraiz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.herraiz.beam.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.JsonToRow.ParseResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Json2RowWithSanitization extends PTransform<PCollection<String>, ParseResult> {
  final Schema schema;

  public Json2RowWithSanitization(Schema s) {
    this.schema = s;
  }

  @Override
  public ParseResult expand(PCollection<String> input) {
    PCollection<String> sanitized =
        input.apply("Sanitize JSON", ParDo.of(new SanitizeJson(schema)));

    return sanitized.apply(
        "Parse", JsonToRow.withExceptionReporting(schema).withExtendedErrorInfo());
  }

  private static class SanitizeJson extends DoFn<String, String> {
    final Schema schema;
    ObjectMapper mapper;

    public SanitizeJson(Schema schema) {
      this.schema = schema;
    }

    @Setup
    public void setup() {
      mapper = new ObjectMapper();
    }

    @ProcessElement
    public void processElement(@Element String jsonStr, OutputReceiver<String> receiver) {
      try {
        JsonNode jsonNode = mapper.readTree(jsonStr);
        JsonNode sanitized = sanitizeNode(jsonNode, schema);
        String sanitizedJsonStr = sanitized.toString();
        receiver.output(sanitizedJsonStr);
      } catch (JsonProcessingException e) {
        // Just pass the string to the next transformation, and let that one report the
        // JSON parsing
        // error
        receiver.output(jsonStr);
      }
    }

    private JsonNode sanitizeNode(JsonNode node, Schema schema) {
      List<Field> fields = schema.getFields();
      ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();

      for (Field f : fields) {
        String fieldName = f.getName();
        JsonNode childNode = node.get(fieldName);
        Field childField = schema.getField(fieldName);
        TypeName typeName = f.getType().getTypeName();

        if (typeName.isCompositeType()) {
          // Recursive iteration if this is a Composite
          Schema childSchema = Objects.requireNonNull(childField.getType().getRowSchema());
          jsonNode.set(fieldName, sanitizeNode(childNode, childSchema));
        } else if (typeName.isCollectionType()) {
          // Add array if this is a collection. We assume all the elements share the same collection type.
          FieldType wrappedType = Objects.requireNonNull(f.getType().getCollectionElementType());
          assert childNode.isArray();
          Stream<JsonNode> stream = StreamSupport.stream(childNode.spliterator(), false);
          List<JsonNode> objects;
          if (wrappedType.getTypeName().isCompositeType() || wrappedType.getTypeName().isCollectionType()) {
            // If the collection type is a Row/Struct, or it is a list of lists
            Schema collectionSchema = Objects.requireNonNull(wrappedType.getRowSchema());
            objects =
                    stream.map(n -> sanitizeNode(n, collectionSchema)).collect(Collectors.toList());
          } else {
            // If the collection type is a single value type
            objects =
                stream.map(n -> sanitizeSingleNode(n, wrappedType)).collect(Collectors.toList());
          }
          ArrayNode sanitizedArray = jsonNode.arrayNode().addAll(objects);
          jsonNode.set(fieldName, sanitizedArray);
        } else {
          // Single type field
          JsonNode sanitized = sanitizeSingleNode(childNode, childField.getType());
          jsonNode.set(fieldName, sanitized);
        }
      }

      return jsonNode;
    }

    private JsonNode sanitizeSingleNode(JsonNode node, FieldType type) {
      TypeName name = type.getTypeName();
      if (name == TypeName.DOUBLE || name == TypeName.FLOAT) {
        return JsonNodeFactory.instance.numberNode(Double.valueOf(node.asText()));
      } else if (name == TypeName.INT64 || name == TypeName.INT32 || name == TypeName.INT16) {
        return JsonNodeFactory.instance.numberNode(Integer.valueOf(node.asText()));
      } else if (name == TypeName.BOOLEAN) {
        return JsonNodeFactory.instance.booleanNode(Boolean.parseBoolean(node.asText()));
      }

      return node;
    }
  }
}
