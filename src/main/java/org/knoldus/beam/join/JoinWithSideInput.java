package org.knoldus.beam.join;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * [[JoinWithSideInput]] class represents a Beam pipeline to perform Left outer join
 * on two data sets - Mall_Customers_Income.csv and Mall_Customers_Scoring.csv with side input pattern
 */
public class JoinWithSideInput {

    private static final Logger LOGGER = LoggerFactory.getLogger(JoinWithSideInput.class);

    public static void main(String[] args) {

        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        LOGGER.info("Reading Mall customers income data");
        PCollection<KV<String, String>> customerIdGenderKV = pipeline.apply("ReadingMallCustomersIncome", TextIO.read()
                        .from("src/main/resources/source/Mall_Customers_Income.csv"))
                .apply("FilterHeader", Filter.by(line -> !line.isEmpty() &&
                        !line.contains("CustomerID,Genre,Age,Annual Income (k$)")))
                .apply("IdGenderKV", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[0], tokens[1]);
                        }));

        LOGGER.info("Reading Mall customers spending scores data");
        PCollection<KV<String, Integer>> customerIdScoreKV = pipeline.apply("ReadingMallCustomersIncome", TextIO.read()
                        .from("src/main/resources/source/Mall_Customers_Scoring.csv"))
                .apply("FilterHeader", Filter.by(line -> !line.isEmpty() &&
                        !line.contains("CustomerID,Spending Score (1-100)")))
                .apply("IdGenderKV", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[0], Integer.parseInt(tokens[1]));
                        }));

       final PCollectionView<Map<String, Integer>> customerScoreView =
                customerIdScoreKV.apply(View.asMap());

        customerIdGenderKV.apply(ParDo.of(new DoFn<KV<String, String>, String>() {

            @ProcessElement
            public void processElement(ProcessContext processContext) {

                Map<String, Integer> customerScores = processContext.sideInput(customerScoreView);
                KV<String, String> element = processContext.element();
                Integer score = customerScores.get(element.getKey());
                LOGGER.info("customerId :{} , Gender: {} , spending score: {}",
                        element.getKey(), element.getValue(), score);

            }

        }).withSideInputs(customerScoreView));

        pipeline.run().waitUntilFinish();
    }
}
