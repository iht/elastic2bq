package dev.herraiz.beam.parser;

// import static
// org.apache.beam.sdk.transforms.JsonToRow.JsonToRowWithErrFn.ERROR_ROW_WITH_ERR_MSG_SCHEMA;
//
// import com.fasterxml.jackson.core.JsonProcessingException;
// import com.fasterxml.jackson.databind.JsonNode;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.google.auto.value.AutoValue;
// import com.google.common.collect.ImmutableMap;
// import dev.herraiz.beam.parser.Json2Row.JsonParseResult;
// import java.util.List;
// import java.util.Map;
// import java.util.stream.Stream;
// import java.util.stream.StreamSupport;
// import org.apache.beam.sdk.Pipeline;
// import org.apache.beam.sdk.schemas.Schema;
// import org.apache.beam.sdk.schemas.Schema.Field;
// import org.apache.beam.sdk.schemas.Schema.FieldType;
// import org.apache.beam.sdk.transforms.DoFn;
// import org.apache.beam.sdk.transforms.PTransform;
// import org.apache.beam.sdk.transforms.ParDo;
// import org.apache.beam.sdk.values.PCollection;
// import org.apache.beam.sdk.values.PCollectionTuple;
// import org.apache.beam.sdk.values.PInput;
// import org.apache.beam.sdk.values.POutput;
// import org.apache.beam.sdk.values.PValue;
// import org.apache.beam.sdk.values.Row;
// import org.apache.beam.sdk.values.TupleTag;
// import org.apache.beam.sdk.values.TupleTagList;

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
//            FieldType collectionType =
//                Objects.requireNonNull(childField.getType().getCollectionElementType());
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

// public class Json2Row extends PTransform<PCollection<String>,
// JsonParseResult> {
//
// final Schema schema;
//
// public Json2Row(Schema schema) {
// this.schema = schema;
// }
//
// @Override
// public JsonParseResult expand(PCollection<String> input) {
//
// PCollectionTuple parsed =
// input.apply(
// "Parse JSON to Row",
// ParDo.of(new Json2RowDoFn(schema))
// .withOutputTags(
// Json2RowDoFn.PARSED_ROWS, TupleTagList.of(Json2RowDoFn.FAILED_ROWS)));
//
// PCollection<Row> parsedRows = parsed.get(Json2RowDoFn.PARSED_ROWS);
// PCollection<Row> failedRows = parsed.get(Json2RowDoFn.FAILED_ROWS);
//
// return JsonParseResult.builder()
// .schema(schema)
// .parsedRows(parsedRows)
// .failedRows(failedRows)
// .callingPipeline(input.getPipeline())
// .build();
// }
//
// @AutoValue
// public abstract static class JsonParseResult implements POutput {
// public JsonParseResult() {}
//
// public abstract PCollection<Row> parsedRows();
//
// public abstract PCollection<Row> failedRows();
//
// abstract Schema schema();
//
// abstract Pipeline callingPipeline();
//
// public Pipeline getPipeline() {
// return this.callingPipeline();
// }
//
// @Override
// public void finishSpecifyingOutput(
// String transformName, PInput input, PTransform<?, ?> transform) {}
//
// public Map<TupleTag<?>, PValue> expand() {
// return ImmutableMap.of(
// Json2RowDoFn.PARSED_ROWS, this.parsedRows(),
// Json2RowDoFn.FAILED_ROWS, this.failedRows());
// }
//
// static JsonParseResult.Builder builder() {
// return new AutoValue_Json2Row_JsonParseResult.Builder();
// }
//
// @AutoValue.Builder
// public abstract static class Builder {
// public Builder() {}
//
// abstract JsonParseResult.Builder parsedRows(PCollection<Row> parsed);
//
// abstract JsonParseResult.Builder failedRows(PCollection<Row> failed);
//
// abstract JsonParseResult.Builder callingPipeline(Pipeline p);
//
// abstract JsonParseResult.Builder schema(Schema s);
//
// abstract JsonParseResult build();
// }
// }
//
// private static class Json2RowDoFn extends DoFn<String, Row> {
//
// static final TupleTag<Row> PARSED_ROWS = new TupleTag<Row>() {};
// static final TupleTag<Row> FAILED_ROWS = new TupleTag<Row>() {};
//
// final Schema schema;
// ObjectMapper objectMapper;
//
// public Json2RowDoFn(Schema schema) {
// this.schema = schema;
// }
//
// @Setup
// public void setup() {
// objectMapper = new ObjectMapper();
// }
//
// @ProcessElement
// public void process(@Element String jsonStr, MultiOutputReceiver receiver) {
// try {
// JsonNode node = objectMapper.readTree(jsonStr);
// Row row = parseFields(node, this.schema);
// receiver.getRowReceiver(PARSED_ROWS).output(row);
// } catch (JsonProcessingException e) {
// Row row =
// Row.withSchema(ERROR_ROW_WITH_ERR_MSG_SCHEMA)
// .withFieldValue("line", jsonStr)
// .withFieldValue("err", e.getMessage())
// .build();
// receiver.getRowReceiver(FAILED_ROWS).output(row);
// }
// }
//
// private Row parseFields(JsonNode node, Schema schema) {
// List<Field> fields = schema.getFields();
//
// Row.Builder rowBuilder = Row.withSchema(schema);
//
// for (Field f : fields) {
// String fieldName = f.getName();
// JsonNode childNode = node.get(fieldName);
// Field childField = schema.getField(fieldName);
// if (f.getType().getTypeName().isCompositeType()) {
// // Recursive iteration if this is a Row
// Schema childSchema = childField.getType().getRowSchema();
// assert childSchema != null;
// rowBuilder.withFieldValue(fieldName, parseFields(childNode, childSchema));
// } else if (f.getType().getTypeName().isCollectionType()) {
// // Add array if this is a collection
// FieldType wrappedType = f.getType().getCollectionElementType();
// assert childNode.isArray();
// Stream<JsonNode> stream = StreamSupport.stream(childNode.spliterator(),
// false);
// List<Object> objects = stream.map(n -> parseField(n, wrappedType)).toList();
// rowBuilder.withFieldValue(fieldName, objects);
// } else {
// // Single type field
// rowBuilder.withFieldValue(fieldName, parseField(childNode,
// childField.getType()));
// }
// }
//
// return rowBuilder.build();
// }
//
// @SuppressWarnings("unchecked")
// private static <T> T parseField(JsonNode node, FieldType type) {
// if (type == FieldType.STRING) {
// return (T) node.asText();
// } else if (type == FieldType.DOUBLE || type == FieldType.FLOAT) {
// return (T) Double.valueOf(node.asDouble());
// } else if (type == FieldType.INT64 || type == FieldType.INT32 || type ==
// FieldType.INT16) {
// return (T) Integer.valueOf(node.asInt());
// } else if (type == FieldType.BYTES) {
// return (T) node.asText(); // FIXME
// } else if (type == FieldType.DECIMAL) {
// return (T) node.asText(); // FIXME
// } else if (type == FieldType.BOOLEAN) {
// return (T) Boolean.valueOf(node.asBoolean()); // FIXME
// } else if (type == FieldType.DATETIME) {
// return (T) node.asText(); // FIXME
// }
//
// return (T) node.asText(); // FIXME
// }
// }
// }
