package dev.herraiz.beam.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class Json2RowDoFn extends DoFn<String, Row> {

  private Schema schema;
  private ObjectMapper objectMapper;

  public Json2RowDoFn(Schema schema) {
    this.schema = schema;
  }

  @Setup
  public void setup() {
    objectMapper = new ObjectMapper();
  }

  @ProcessElement
  public void process(@Element String jsonStr, OutputReceiver<Row> receiver) {

  }

  private Row parseFields(JsonNode node, Schema schema) throws Exception {
    List<Field> fields = schema.getFields();

    Row row = Row.withSchema(schema).build();

    for (Field f : fields) {
      if (f.getType().getTypeName().isCompositeType()) {
        // Recursive iteration
      } else if (f.getType().getTypeName().isCollectionType()) {
        FieldType wrappedType = f.getType().getCollectionElementType();
        JsonNode childNode = node.get(f.getName());
        assert childNode.isArray();
        List<Object> objects = StreamSupport.stream(childNode.spliterator(), false)
            .map(n -> parseField(n, wrappedType)).toList();
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static <T> T parseField(JsonNode node, FieldType type) throws Exception {
    if (type == FieldType.STRING) {
      return (T) node.asText();
    } else if (type == FieldType.DOUBLE || type == FieldType.FLOAT) {
      return (T) Double.valueOf(node.asDouble());
    } else if (type == FieldType.INT64 || type == FieldType.INT32 || type == FieldType.INT16) {
      return (T) Integer.valueOf(node.asInt());
    } else if (type == FieldType.BYTES) {
      throw new Exception("BYTES unimplemented");  // FIXME
    } else if (type == FieldType.DECIMAL) {
      throw new Exception("DECIMAL unimplemented");  // FIXME
    } else if (type == FieldType.BOOLEAN) {
      return (T) Boolean.valueOf(node.asBoolean());  // FIXME
    } else if (type == FieldType.DATETIME) {
      throw new Exception("DATETIME unimplemented");  // FIXME
    }

    return null;
  }


}
