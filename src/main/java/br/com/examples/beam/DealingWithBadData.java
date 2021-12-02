package br.com.examples.beam;

import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import br.com.examples.beam.WordCount.ExtractWordsFn;
import br.com.examples.model.User;

public class DealingWithBadData {

	private static final Logger LOG = LoggerFactory.getLogger(DealingWithBadData.class);
	private static final TupleTag<String> SUCCESS = new TupleTag<String>() {
	};
	private static final TupleTag<String> INVALID = new TupleTag<String>() {
	};

	public interface Options extends PipelineOptions {

		@Description("Path of the file to read from")
		@Required
		String getInputFile();

		void setInputFile(String value);

		@Description("output file path")
		@Required
		String getOutput();

		void setOutput(String output);

		@Description("dead Letter file path")
		@Required
		String getDeadLetter();

		void setDeadLetter(String deadLetter);
	}

	static class CheckElements extends DoFn<String, String> {

		@ProcessElement
		public void processElement(ProcessContext c) {
			String s = c.element();
			ObjectMapper mapper = new ObjectMapper();
			try {
				User user = mapper.readValue(s, User.class);
				c.output(SUCCESS, user.toString());
			} catch (Exception e) {
				LOG.error("json parse error", e);
				c.output(INVALID, c.element());
			}
		}
	}

	static void runPipeline(Options options) {
		Pipeline p = Pipeline.create(options);

		PCollection<String> input = p.apply("ReadFile", TextIO.read().from(options.getInputFile()));

		PCollectionTuple outputTuple = input.apply("transform",
				ParDo.of(new CheckElements()).withOutputTags(SUCCESS, TupleTagList.of(INVALID)));

		outputTuple.get(SUCCESS).apply(TextIO.write().to(options.getOutput()));
		outputTuple.get(INVALID).apply(TextIO.write().to(options.getDeadLetter()));

		p.run().waitUntilFinish();
	}

	public static void main(String[] args) {

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		runPipeline(options);
	}

}
