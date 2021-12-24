package org.knoldus.beam.join;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [[CoGroupByKeyJoin]] class represents a Beam pipeline to perform join using CoGroupByKey
 * Beam transformation on two data sets - Mall_Customers_Income.csv and Mall_Customers_Scoring.csv
 */
public class CoGroupByKeyJoin {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoGroupByKeyJoin.class);

    private static final String CSV_HEADER_INCOME = "CustomerID,Genre,Age,Annual Income (k$)";
    private static final String CSV_HEADER_SCORING = "CustomerID,Spending Score (1-100)";

    public static void main(String[] args) {

        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        LOGGER.info("Reading Mall customers income data");
        //TODO: Move this to reuse in another classes
        PCollection<KV<Integer, Integer>> customerIncome = pipeline.apply("ReadMallCustomerIncome",
                        TextIO.read().from("src/main/resources/source/Mall_Customers_Income.csv"))
                .apply("FilterHeader", Filter.by((String line) -> !line.isEmpty() && !line.equals(CSV_HEADER_INCOME)))
                .apply("KvCustomerIDIncome", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.integers()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[3]));
                        }));

        LOGGER.info("Reading Mall customers spending scores data");
        //TODO: Move this to reuse in another classes
        PCollection<KV<Integer, Integer>> customerScoring = pipeline.apply("ReadMallCustomerScoring",
                        TextIO.read().from("src/main/resources/source/Mall_Customers_Scoring.csv"))
                .apply("FilterHeader", Filter.by((String line) -> !line.isEmpty() && !line.equals(CSV_HEADER_SCORING)))
                .apply("KvCustomerIDScoring", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.integers()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
                        }));

        TupleTag<Integer> incomeTag = new TupleTag<>();
        TupleTag<Integer> scoreTag = new TupleTag<>();

        PCollection<KV<Integer, CoGbkResult>> joinedResult = KeyedPCollectionTuple
                .of(incomeTag, customerIncome)
                .and(scoreTag, customerScoring)
                .apply(CoGroupByKey.create());

        joinedResult.apply("JoinedResult", MapElements
                .into(TypeDescriptors.voids())
                .via((KV<Integer, CoGbkResult> joinedStream) -> {
                    LOGGER.info("customerId :{} , Customer income: {} , spending score: {}",
                            joinedStream.getKey(), joinedStream.getValue().getOnly(incomeTag, 0),
                            joinedStream.getValue().getOnly(scoreTag, 0));
                    return null;
                }));

        pipeline.run().waitUntilFinish();
    }
}
