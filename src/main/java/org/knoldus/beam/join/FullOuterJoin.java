package org.knoldus.beam.join;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [[FullOuterJoin]] class represents a Beam pipeline to perform full outer join
 * on two data sets - Mall_Customers_Income.csv and Mall_Customers_Scoring.csv
 */
public final class FullOuterJoin {

    private static final Logger LOGGER = LoggerFactory.getLogger(FullOuterJoin.class);

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
        PCollection<KV<String, Integer>> customerIdScoreKV = pipeline.apply("ReadingMallCustomersScore", TextIO.read()
                .from("src/main/resources/source/Mall_Customers_Scoring.csv"))
                .apply("FilterHeader", Filter.by(line -> !line.isEmpty() &&
                        !line.contains("CustomerID,Spending Score (1-100)")))
                .apply("IdGenderKV", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[0], Integer.parseInt(tokens[1]));
                        }));

        LOGGER.info("Full Outer Join of customerIdGenderKV  and customerIdScoreKV PCollection");
        Join.fullOuterJoin(customerIdGenderKV, customerIdScoreKV, "others", -1)
                .apply("JoinedData", MapElements
                        .into(TypeDescriptors.voids())
                        .via((KV<String, KV<String, Integer>> joinedStream) -> {

                            LOGGER.info("customerId :{} , Gender: {} , spending score: {}",
                                    joinedStream.getKey(), joinedStream.getValue().getKey(), joinedStream.getValue().getValue());
                            return null;
                        }));

        pipeline.run().waitUntilFinish();
    }
}
