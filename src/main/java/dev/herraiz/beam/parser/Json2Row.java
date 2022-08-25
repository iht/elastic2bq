package dev.herraiz.beam.parser;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class Json2Row extends DoFn<String, Row> {

    @ProcessElement
    public void process(@Element String jsonStr, OutputReceiver<Row> receiver) {

    }
}
