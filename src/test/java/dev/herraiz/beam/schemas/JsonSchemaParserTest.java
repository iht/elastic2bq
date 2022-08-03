package dev.herraiz.beam.schemas;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.Before;
import org.junit.Test;

public class JsonSchemaParserTest {

  private Schema schema;

  @Before
  public void setUp() throws IOException {
    String schemaPath = "src/test/resources/schemas/github_commits.json";
    String schemaString = Files.readString(Path.of(schemaPath));
    schema = JsonSchemaParser.bqJson2BeamSchema(schemaString);
  }

  @Test
  public void validateNumberOfColumns() {
    assertEquals("The table has 12 columns", 12, schema.getFieldCount());
  }

  @Test
  public void validateAuthorColumn() {
    FieldType authorType = schema.getField("author").getType();
    Schema author = authorType.getRowSchema();
    assertNotNull("Author column exists", author);
    assertEquals("Author has 5 nested columns", 5, author.getFieldCount());
    assertNull("Author is not an array", authorType.getCollectionElementType());
  }

  @Test
  public void validateCommitterColumn() {
    FieldType committerType = schema.getField("committer").getType();
    Schema committer = committerType.getRowSchema();
    assertNotNull("Committer column exists", committer);
    assertEquals("Committer has 5 nested columns", 5, committer.getFieldCount());
    assertNull("Comitter is not an array", committerType.getCollectionElementType());

  }

  @Test
  public void validateTrailerColumn() {
    FieldType trailerType = schema.getField("trailer").getType();
    assertNotNull("Trailer column exists", trailerType);
    FieldType wrappedType = trailerType.getCollectionElementType();
    assertNotNull("Trailer is an array", wrappedType);
    Schema trailer = wrappedType.getRowSchema();
    assertNotNull("Trailer is an array of non nulls", trailer);
    assertEquals("Trailer has 3 nested columns", 3, trailer.getFieldCount());
  }
}