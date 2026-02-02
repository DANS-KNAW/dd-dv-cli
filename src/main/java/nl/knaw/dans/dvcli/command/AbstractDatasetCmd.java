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

import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lib.dataverse.DatasetApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@RequiredArgsConstructor
public abstract class AbstractDatasetCmd {
    private final DataverseClient dataverseClient;

    @Parameters(index = "0", paramLabel = "PID", description = "PID of the dataset")
    protected String pid;

    protected DatasetApi getDatasetApi() {
        // If parseable as a number, DataverseClient.dataset(long) could be used
        try {
            int id = Integer.parseInt(pid);
            return dataverseClient.dataset(id);
        }
        catch (NumberFormatException e) {
            // not a number, fall through
        }
        return dataverseClient.dataset(pid);
    }

}
