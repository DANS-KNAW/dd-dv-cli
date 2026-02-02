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
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(
    name = "dataset-lock-add",
    description = "Adds a lock to a dataset",
    mixinStandardHelpOptions = true
)
public class DatasetLockAdd extends AbstractDatasetCmd implements Callable<Integer> {
    @Parameters(index = "1", paramLabel = "LOCK_TYPE", description = "Type of lock to add (e.g., 'Ingest', 'Workflow')")
    private String lockType;

    public DatasetLockAdd(DataverseClient dataverseClient) {
        super(dataverseClient);
    }

    public Integer call() {
        try {
            var response = getDatasetApi().addLock(lockType);
            System.out.println(response.getEnvelopeAsString());
            return 0;
        }
        catch (Exception e) {
            System.err.println("Error adding lock to dataset: " + e.getMessage());
            return 1;
        }
    }

}
