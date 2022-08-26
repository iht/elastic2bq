package dev.herraiz.beam.parser;

import static org.apache.beam.sdk.transforms.JsonToRow.JsonToRowWithErrFn.ERROR_ROW_WITH_ERR_MSG_SCHEMA;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

public class Json2RowDoFn extends DoFn<String, Row> {

  private final TupleTag<Row> parsedRows = new TupleTag<>();
  private final TupleTag<Row> failedRows = new TupleTag<>();

  private final Schema schema;
  private ObjectMapper objectMapper;

  public Json2RowDoFn(Schema schema) {
    this.schema = schema;
  }

  @Setup
  public void setup() {
    objectMapper = new ObjectMapper();
  }

  @ProcessElement
  public void process(@Element String jsonStr, MultiOutputReceiver receiver) {
    try {
      JsonNode node = objectMapper.readTree(jsonStr);
      Row row = parseFields(node, this.schema);
      receiver.getRowReceiver(parsedRows).output(row);
    } catch (JsonProcessingException e) {
      Row row =
          Row.withSchema(ERROR_ROW_WITH_ERR_MSG_SCHEMA)
              .withFieldValue("line", jsonStr)
              .withFieldValue("err", e.getMessage())
              .build();
      receiver.getRowReceiver(failedRows).output(row);
    }
  }

  private Row parseFields(JsonNode node, Schema schema) {
    List<Field> fields = schema.getFields();

    Row.Builder rowBuilder = Row.withSchema(schema);

    for (Field f : fields) {
      String fieldName = f.getName();
      JsonNode childNode = node.get(fieldName);
      Field childField = schema.getField(fieldName);
      if (f.getType().getTypeName().isCompositeType()) {
        // Recursive iteration if this is a Row
        Schema childSchema = childField.getType().getRowSchema();
        assert childSchema != null;
        rowBuilder.withFieldValue(fieldName, parseFields(childNode, childSchema));
      } else if (f.getType().getTypeName().isCollectionType()) {
        // Add array if this is a collection
        FieldType wrappedType = f.getType().getCollectionElementType();
        assert childNode.isArray();
        Stream<JsonNode> stream = StreamSupport.stream(childNode.spliterator(), false);
        List<Object> objects = stream.map(n -> parseField(n, wrappedType)).toList();
        rowBuilder.withFieldValue(fieldName, objects);
      } else {
        // Single type field
        rowBuilder.withFieldValue(fieldName, parseField(childNode, childField.getType()));
      }
    }

    return rowBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private static <T> T parseField(JsonNode node, FieldType type) {
    if (type == FieldType.STRING) {
      return (T) node.asText();
    } else if (type == FieldType.DOUBLE || type == FieldType.FLOAT) {
      return (T) Double.valueOf(node.asDouble());
    } else if (type == FieldType.INT64 || type == FieldType.INT32 || type == FieldType.INT16) {
      return (T) Integer.valueOf(node.asInt());
    } else if (type == FieldType.BYTES) {
      return (T) node.asText(); // FIXME
    } else if (type == FieldType.DECIMAL) {
      return (T) node.asText(); // FIXME
    } else if (type == FieldType.BOOLEAN) {
      return (T) Boolean.valueOf(node.asBoolean()); // FIXME
    } else if (type == FieldType.DATETIME) {
      return (T) node.asText(); // FIXME
    }

    return (T) node.asText(); // FIXME
  }
}
