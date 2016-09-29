/**
 * Copyright 2012 Netflix, Inc.
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
package com.netflix.turbine.discovery;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;

/**
 * Simple class that reads out the contents form a file (line at a time) and uses that info to represent {@link Instance} info
 * <p>Format of the file must be of the form
 * <br/>
 * [<b>hostname</b>],[<b>clusterName</b>],[<b>up/down</b>]
 * 
 */
public class FileBasedInstanceDiscovery implements InstanceDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(FileBasedInstanceDiscovery.class);

    private final DynamicStringProperty filePath = DynamicPropertyFactory.getInstance().getStringProperty("turbine.FileBasedInstanceDiscovery.filePath", "");

    private final File file; 
    
    public FileBasedInstanceDiscovery() {
        file = new File(filePath.get());
    }
    
    @Override
    public Collection<Instance> getInstanceList() {
        
        List<String> lines = null;
        try {
            lines = FileUtils.readLines(file);
        } catch (IOException e) {
            logger.error("Error reading from file, check config property: " + filePath.getName() + "=" + filePath.get(), e);
        }
        
        List<Instance> instances = new ArrayList<Instance>();
        if (lines != null) {
            for (String line : lines) {
                try {
                    instances.add(parseInstance(line));
                } catch (Exception e) {
                    logger.error("Error reading from file: " + filePath.get(), e);
                }
            }
        }
        return instances;
    }

    private Instance parseInstance(String line) throws Exception {
     
        String[] parts = line.split(",");
        if (parts == null || parts.length != 3) {
            throw new Exception("Bad file format: " + line);
        }
        
        return new Instance(parts[0], parts[1], parts[2].equalsIgnoreCase("up"));
    }
}
