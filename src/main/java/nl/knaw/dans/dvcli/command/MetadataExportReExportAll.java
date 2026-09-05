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

import java.util.concurrent.Callable;

@Command(name = "metadata-export-re-export-all",
         description = "Forces re-export of all published local datasets",
         mixinStandardHelpOptions = true)
@RequiredArgsConstructor
public class MetadataExportReExportAll implements Callable<Integer> {
    private final MetadataExportApi metadataExportApi;

    @Option(names = { "--older-than" }, description = "Only re-export datasets exported before this date (YYYY-MM-DD)")
    private String olderThan;

    @Option(names = { "--formats" }, split = ",", description = "Comma-separated metadata formats to export")
    private String[] formats;

    @Override
    public Integer call() throws Exception {
        try {
            var response = metadataExportApi.reExportAll(olderThan, formats);
            System.out.println(response.getEnvelopeAsString());
            return 0;
        }
        catch (DataverseException e) {
            System.err.println("Error re-exporting metadata: " + e.getMessage());
            return 1;
        }
    }
}
