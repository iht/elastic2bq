/*
 * Copyright 2022 Israel Herraiz. 
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
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

package dev.herraiz.beam.schemas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;

/** Transform a BigQuery JSON string into a Beam schema. */
public class JsonSchemaParser {

    private static final String ROOT_NODE_PATH = "schema";

    public static Schema bqJson2BeamSchema(String schemaAsString) throws Exception {
        JsonFactory defaultJsonFactory = Utils.getDefaultJsonFactory();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode topNode = mapper.readTree(schemaAsString);
        JsonNode schemaRootNode = topNode.path(ROOT_NODE_PATH);
        if (schemaRootNode.isMissingNode()) {
            throw new Exception(
                    "Is this a BQ schema? The given schema must have a top node of name "
                            + ROOT_NODE_PATH);
        }

        TableSchema tableSchema =
                defaultJsonFactory.fromString(schemaRootNode.toString(), TableSchema.class);

        return BigQueryUtils.fromTableSchema(tableSchema);
    }
}
