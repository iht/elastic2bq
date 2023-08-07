/*
 * Copyright 2023 Israel Herraiz.
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

package dev.herraiz.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableSchema;
import dev.herraiz.beam.schemas.JsonSchemaInferrer;
import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class InferSchemaFromData {
    public static void main(String[] args) throws Exception {
        DefaultParser parser = new DefaultParser();
        Options options = createCommandLineOptions();
        CommandLine cli = parser.parse(options, args);

        if (cli.hasOption("help")) {
            showHelp(options);
            System.exit(0);
        }

        String sampleDataLocation = cli.getOptionValue("data");
        String outputFilename = cli.getOptionValue("output");
        String bigQueryProject = cli.getOptionValue("project");
        String bigQueryDataset = cli.getOptionValue("dataset");

        File outputFile = Path.of(outputFilename).toFile();
        if (outputFile.exists()) {
            System.out.println("The output file exists. Refusing to overwrite.");
            System.exit(1);
        }

        inferSchemaFromData(sampleDataLocation, outputFile, bigQueryProject, bigQueryDataset);
    }

    private static Options createCommandLineOptions() {
        Options options = new Options();

        Option data =
                Option.builder("d")
                        .longOpt("data")
                        .argName("GCS SAMPLE DATA LOCATION")
                        .hasArg()
                        .desc("Filename to write the inferred schema to")
                        .build();

        Option help = new Option("h", "help", false, "Show the help message");

        Option output =
                Option.builder("o")
                        .longOpt("output")
                        .argName("FILENAME")
                        .hasArg()
                        .desc("Filename to write the inferred schema to")
                        .build();

        Option project =
                Option.builder("p")
                        .longOpt("project")
                        .argName("PROJECT ID")
                        .hasArg()
                        .desc("BigQuery project used to import data")
                        .build();

        Option dataset =
                Option.builder("ds")
                        .longOpt("dataset")
                        .argName("DATASET ID")
                        .hasArg()
                        .desc("BigQuery dataset used to import data")
                        .build();

        options.addOption(help);
        options.addOption(data);
        options.addOption(output);
        options.addOption(project);
        options.addOption(dataset);

        return options;
    }

    private static void showHelp(Options options) {
        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("InferSchemaFromData", options);
    }

    private static void inferSchemaFromData(
            String dataLocation, File outputFile, String bigQueryProject, String bigQueryDataset)
            throws Exception {
        TableSchema tableSchema =
                JsonSchemaInferrer.inferSchemaFromSample(
                        dataLocation, bigQueryProject, bigQueryDataset);

        Map<String, TableSchema> m = new HashMap<>();
        m.put("schema", tableSchema);

        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(outputFile, m);
    }
}
