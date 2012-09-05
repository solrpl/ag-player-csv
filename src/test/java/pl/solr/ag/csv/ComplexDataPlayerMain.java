/**
 * Copyright 2012 Solr.pl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
package pl.solr.ag.csv;

import pl.solr.ag.csv.sink.ComplexDataSink;

import com.sematext.ag.PlayerConfig;
import com.sematext.ag.PlayerRunner;
import com.sematext.ag.player.RealTimePlayer;
import com.sematext.ag.source.FiniteEventSource;
import com.sematext.ag.source.SimpleSourceFactory;
import com.sematext.ag.source.dictionary.ComplexEventSource;

/**
 * Command line util for generate random data to CSV file.
 * 
 * @author negativ
 *
 */
public class ComplexDataPlayerMain {
	private ComplexDataPlayerMain() {
	}

	public static void main(final String[] args) {
		if (args.length < 4) {
			System.out.println("Usage: outputFile fieldsOrder eventsCount schemaFile");
			System.out.println("Example: /tmp/output.csv id,name,type 100 schema.json");
			System.exit(1);
		}

		String outputFileName = args[0];
		String fieldsOrder = args[1];
		String eventsCount = args[2];
		String schemaFile = args[3];
		
		PlayerConfig config = new PlayerConfig(
				SimpleSourceFactory.SOURCE_CLASS_CONFIG_KEY,
				ComplexEventSource.class.getName(),
				RealTimePlayer.SOURCES_PER_THREAD_COUNT_KEY, "1",
				RealTimePlayer.SOURCES_THREADS_COUNT_KEY, "1",
				FiniteEventSource.MAX_EVENTS_KEY, eventsCount,
				ComplexEventSource.SCHEMA_FILE_NAME_KEY, schemaFile,
				PlayerRunner.SINK_CLASS_CONFIG_KEY, ComplexDataSink.class.getName(),
				ComplexDataSink.OUTPUT_FILE_NAME_KEY, outputFileName,
				ComplexDataSink.FIELD_ORDER_KEY, fieldsOrder);
		PlayerRunner.play(config);
	}
}
