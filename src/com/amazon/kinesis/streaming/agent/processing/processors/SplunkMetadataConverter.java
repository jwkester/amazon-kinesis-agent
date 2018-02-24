/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file.
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazon.kinesis.streaming.agent.processing.processors;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.Logging;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IJSONPrinter;
import com.amazon.kinesis.streaming.agent.processing.utils.ProcessingUtilsFactory;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Build record as JSON object with arbitrary KV pairs
 *   and "event" key with the raw data to be sent to Splunk
 *
 * Configuration looks like:
 *
 * {
 *   "optionName": "SPLUNKMETADATA",
 *   "splunkdata": {
 *     "key": "value",
 *     "key2": "value2"
 *   }
 * }
 *
 * @author jkester
 *
 */
public class SplunkMetadataConverter implements IDataConverter {

    private Map<String, Object>  splunkdata;
    private final IJSONPrinter jsonProducer;
    private static final Logger LOGGER = Logging.getLogger(AddEC2MetadataConverter.class);

    public SplunkMetadataConverter(Configuration config) {
        splunkdata = (Map<String, Object>) config.getConfigMap().get("splunkdata");
        jsonProducer = ProcessingUtilsFactory.getPrinter(config);
    }

    @Override
    public ByteBuffer convert(ByteBuffer data) throws DataConversionException {

        final Map<String, Object> recordMap = new LinkedHashMap<String, Object>();
        String dataStr = ByteBuffers.toString(data, StandardCharsets.UTF_8);

        if (dataStr.endsWith(NEW_LINE)) {
            dataStr = dataStr.substring(0, (dataStr.length() - NEW_LINE.length()));
        }

        for (Map.Entry<String, Object> entry : splunkdata.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            recordMap.put(key, value);
        }

        recordMap.put("event", dataStr);

        String dataJson = jsonProducer.writeAsString(recordMap) + NEW_LINE;
        return ByteBuffer.wrap(dataJson.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}