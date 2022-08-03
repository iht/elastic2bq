package dev.herraiz.beam.schemas;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import org.apache.beam.sdk.schemas.Schema;
import org.junit.Before;
import org.junit.Test;

public class JsonSchemaParserTest {

  private String schemaString;

  @Before
  public void setUp() throws Exception {
    String schemaPath = "src/test/resources/schemas/github_commits.json";
    schemaString = Files.readString(Path.of(schemaPath));
  }

  @Test
  public void parseSchema() throws JsonProcessingException {
    Schema schema = JsonSchemaParser.bqJson2BeamSchema(schemaString);
    assertEquals("The table has 12 columns",12, schema.getFieldCount());
  }
}