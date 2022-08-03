package dev.herraiz.beam.schemas;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.Before;
import org.junit.Test;

public class JsonSchemaParserTest {

  private Schema schema;

  @Before
  public void setUp() throws Exception {
    String schemaPath = "src/test/resources/schemas/github_commits.json";
    String schemaString = Files.readString(Path.of(schemaPath));
    schema = JsonSchemaParser.bqJson2BeamSchema(schemaString);
  }

  @Test
  public void numberOfColumns() {
    assertEquals("The table has 12 columns", 12, schema.getFieldCount());
  }

  @Test
  public void authorColumn() {
    FieldType authorType = schema.getField("author").getType();
    Schema author = authorType.getRowSchema();
    assertNotNull("Author column exists", author);
    assertEquals("Author has 5 nested columns", 5, author.getFieldCount());
    assertNull("Author is not an array", authorType.getCollectionElementType());
  }

  @Test
  public void committerColumn() {
    FieldType committerType = schema.getField("committer").getType();
    Schema committer = committerType.getRowSchema();
    assertNotNull("Committer column exists", committer);
    assertEquals("Committer has 5 nested columns", 5, committer.getFieldCount());
    assertNull("Committer is not an array", committerType.getCollectionElementType());
  }

  @Test
  public void trailerColumn() {
    FieldType trailerType = schema.getField("trailer").getType();
    assertNotNull("Trailer column exists", trailerType);
    FieldType wrappedType = trailerType.getCollectionElementType();
    assertNotNull("Trailer is an array", wrappedType);
    Schema trailer = wrappedType.getRowSchema();
    assertNotNull("Trailer is an array of non nulls", trailer);
    assertEquals("Trailer has 3 nested columns", 3, trailer.getFieldCount());
  }

  @Test
  public void differenceColumn() {
    FieldType trailerType = schema.getField("difference").getType();
    assertNotNull("Difference column exists", trailerType);
    FieldType wrappedType = trailerType.getCollectionElementType();
    assertNotNull("Difference is an array", wrappedType);
    Schema trailer = wrappedType.getRowSchema();
    assertNotNull("Difference is an array of non nulls", trailer);
    assertEquals("Trailer has 8 nested columns", 8, trailer.getFieldCount());
  }

  @Test
  public void arrayVsNonArrayColumns() {
    FieldType nonArray = schema.getField("tree").getType().getCollectionElementType();
    FieldType array = schema.getField("parent").getType().getCollectionElementType();
    assertNull("Tree is not an array", nonArray);
    assertNotNull("Parent is an array", array);
    assertEquals("Parent is an array of strings", FieldType.STRING, array);
  }

  @Test
  public void nullableVsNonNullableColumns() {
    Boolean commit = schema.getField("commit").getType().getNullable();
    Boolean tree = schema.getField("tree").getType().getNullable();
    assertTrue("Commit is a nullable column", commit);
    assertFalse("Tree is a required column", tree);
  }
}