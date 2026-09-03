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

import nl.knaw.dans.lib.dataverse.DataverseHttpResponse;
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.SolrIndexApi;
import nl.knaw.dans.lib.dataverse.model.DataMessageSolrIndex;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(name = "index-dataset",
         description = "Indexes a dataset by database id or persistent identifier",
         mixinStandardHelpOptions = true)
@RequiredArgsConstructor
public class IndexDataset implements Callable<Integer> {
    private final SolrIndexApi solrIndexApi;

    @Parameters(index = "0", paramLabel = "DATASET_ID_OR_PID", description = "Database id or persistent identifier of the dataset")
    private String datasetIdOrPid;

    @Override
    public Integer call() throws Exception {
        try {
            var response = indexDataset();
            System.out.println(response.getEnvelopeAsString());
            return 0;
        }
        catch (DataverseException e) {
            System.err.println("Error indexing dataset: " + e.getMessage());
            return 1;
        }
    }

    private DataverseHttpResponse<DataMessageSolrIndex> indexDataset() throws Exception {
        try {
            return solrIndexApi.indexDataset(Integer.parseInt(datasetIdOrPid));
        }
        catch (NumberFormatException e) {
            return solrIndexApi.indexDataset(datasetIdOrPid);
        }
    }
}
