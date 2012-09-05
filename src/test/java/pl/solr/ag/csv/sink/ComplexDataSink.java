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
package pl.solr.ag.csv.sink;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import pl.solr.dm.DataType;

import com.sematext.ag.PlayerConfig;
import com.sematext.ag.event.ComplexEvent;
import com.sematext.ag.exception.InitializationFailedException;
import com.sematext.ag.sink.Sink;

/**
 * Action Generator sink for CSV files.
 *
 * @author negativ
 *
 */
public class ComplexDataSink extends Sink<ComplexEvent> {
	public static final String OUTPUT_FILE_NAME_KEY = "complexDataCsvSink.outputFile";
	public static final String FIELD_ORDER_KEY = "complexDataCsvSink.fieldOrder";
	private OutputStream output;
	private String[] fieldOrder;
	
	@Override
	public void init(PlayerConfig config) throws InitializationFailedException {
		super.init(config);

		String outputFile = config.get(OUTPUT_FILE_NAME_KEY);

		if (outputFile == null || "".equals(outputFile.trim())) {
			throw new IllegalArgumentException(this.getClass().getName()
					+ " expects configuration property " + OUTPUT_FILE_NAME_KEY);
		}
		
		String fields = config.get(FIELD_ORDER_KEY);

		if (fields == null || "".equals(fields.trim())) {
			throw new IllegalArgumentException(this.getClass().getName()
					+ " expects configuration property " + FIELD_ORDER_KEY);
		}
		
		fieldOrder = fields.split(",");
		
		try {
			output = new FileOutputStream(outputFile);
		} catch (Exception e) {
			throw new InitializationFailedException(e.getMessage());
		}
	}

	@Override
	public boolean write(ComplexEvent event) {
		Map<String, DataType<?>> data = event.getObject().getValue();
		StringBuilder builder = new StringBuilder();
		for (String key : fieldOrder) {
			if (builder.length() > 0) {
				builder.append(";");
			}
			if (data.containsKey(key)) {
				builder.append(data.get(key).getValue());
			}
		}
		builder.append("\n");
		try {
			output.write(builder.toString().getBytes());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return true;
	}

	@Override
	public void close() {
		if (output != null) {
			try {
				output.close();
			} catch (IOException e) {
				// ignored
			}
		}
		super.close();
	}

}
