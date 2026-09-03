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
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.SolrIndexApi;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(name = "index-dataverse",
         description = "Indexes a dataverse by database id",
         mixinStandardHelpOptions = true)
@RequiredArgsConstructor
public class IndexDataverse implements Callable<Integer> {
    private final SolrIndexApi solrIndexApi;

    @Parameters(index = "0", paramLabel = "DATAVERSE_ID", description = "Database id of the dataverse")
    private int dataverseId;

    @Override
    public Integer call() throws Exception {
        try {
            var response = solrIndexApi.indexDataverse(dataverseId);
            System.out.println(response.getEnvelopeAsString());
            return 0;
        }
        catch (DataverseException e) {
            System.err.println("Error indexing dataverse: " + e.getMessage());
            return 1;
        }
    }
}
