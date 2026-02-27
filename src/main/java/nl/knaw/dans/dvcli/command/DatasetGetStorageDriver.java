/*
 * Copyright (C) 2026 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.dvcli.command;

import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

@Command(name = "dataset-get-storage-driver",
         description = "Gets the storage driver for a dataset",
         mixinStandardHelpOptions = true)
public class DatasetGetStorageDriver extends AbstractDatasetCmd implements Callable<Integer> {

    public DatasetGetStorageDriver(DataverseClient dataverseClient) {
        super(dataverseClient);
    }

    @Override
    public Integer call() throws Exception {
        try {
            var response = getDatasetApi().getStorageDriver();
            var driver = response.getData();
            if (driver.getLabel() != null) {
                System.out.printf("%-10s %s%n", "LABEL:", driver.getLabel());
            } else {
                System.err.println("No label returned (older version of Dataverse)");
            }
            System.out.printf("%-10s %s%n", "ID:", driver.getName() == null ? driver.getMessage() : driver.getName());
            return 0;
        }
        catch (DataverseException e) {
            System.err.println("Error getting storage driver for dataset: " + e.getMessage());
            return 1;
        }
    }
}
