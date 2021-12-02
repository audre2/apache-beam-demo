package br.com.examples.beam;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import br.com.examples.beam.DealingWithBadData.CheckElements;

public class DealingWithBadDataTest {

	@Rule
	public final transient TestPipeline p = TestPipeline.create();

	@Test
	@Category(NeedsRunner.class)
	public void testGenerateAnagramsFn() {
	    // Create the test input
	    PCollection<String> words = p.apply(Create.of("friend"));

	    // Test a single DoFn using the test input
	    PCollection<String> anagrams = 
	        words.apply("Generate anagrams", ParDo.of(new CheckElements()));

	    // Assert correct output from
	    PAssert.that(anagrams).containsInAnyOrder(
	        "finder", "friend", "redfin", "refind");

	    p.run();
	}

	
}
