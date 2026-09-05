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
import nl.knaw.dans.lib.dataverse.MetadataExportApi;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(name = "metadata-export-dataset",
         description = "Forces re-export of a specific dataset",
         mixinStandardHelpOptions = true)
@RequiredArgsConstructor
public class MetadataExportDataset implements Callable<Integer> {
    private final MetadataExportApi metadataExportApi;

    @Parameters(index = "0", paramLabel = "DATASET_ID_OR_PID", description = "Dataset database id or persistent identifier")
    private String datasetIdOrPid;

    @Option(names = { "--formats" }, split = ",", description = "Comma-separated metadata formats to export")
    private String[] formats;

    @Override
    public Integer call() throws Exception {
        try {
            var response = reExportDataset();
            System.out.println(response.getEnvelopeAsString());
            return 0;
        }
        catch (DataverseException e) {
            System.err.println("Error re-exporting dataset metadata: " + e.getMessage());
            return 1;
        }
    }

    private nl.knaw.dans.lib.dataverse.DataverseHttpResponse<Object> reExportDataset() throws Exception {
        try {
            long id = Long.parseLong(datasetIdOrPid);
            if (id >= 0 && id <= Integer.MAX_VALUE) {
                return metadataExportApi.reExportDataset((int) id, formats);
            }
            return metadataExportApi.reExportDataset(datasetIdOrPid, formats);
        }
        catch (NumberFormatException e) {
            return metadataExportApi.reExportDataset(datasetIdOrPid, formats);
        }
    }
}
