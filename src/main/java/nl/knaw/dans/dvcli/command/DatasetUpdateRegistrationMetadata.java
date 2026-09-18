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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.DatasetApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(name = "dataset-update-registration-metadata",
         description = "Updates registration metadata for a published dataset or batch of datasets at the PID provider",
         mixinStandardHelpOptions = true)
@Slf4j
public class DatasetUpdateRegistrationMetadata implements Callable<Integer> {
    private static final String PID = "pid";

    @Option(names = { "-i", "--input-file" }, description = "Input CSV file")
    private Path inputFile;

    @Parameters(arity = "0..1", paramLabel = "PID", description = "PID of the dataset")
    private String pid;

    private final DataverseClient dataverseClient;

    public DatasetUpdateRegistrationMetadata(DataverseClient dataverseClient) {
        this.dataverseClient = dataverseClient;
    }

    @Override
    public Integer call() {
        try {
            if (inputFile == null && isBlank(pid)) {
                System.err.println("Either --input-file or PID is required");
                return 1;
            }

            if (inputFile == null) {
                var response = getDatasetApi(pid).updateRegistrationMetadata();
                System.out.println(response.getEnvelopeAsString());
                return 0;
            }

            try (var writer = createReportWriter();
                var batchProcessor = BatchProcessor.forCsv(inputFile, writer)) {
                var summary = batchProcessor.process(this::processRow);
                log.info("Finished: {} ok, {} failed, {} skipped", summary.getOkCount(), summary.getFailedCount(), summary.getSkippedCount());
                return summary.getFailedCount() == 0 ? 0 : 1;
            }
        }
        catch (Exception e) {
            log.error("Error updating registration metadata", e);
            System.err.println("Error updating registration metadata: " + e.getMessage());
            return 1;
        }
    }

    private Writer createReportWriter() {
        return new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
    }

    private BatchProcessor.Result processRow(BatchProcessor.Row row) throws Exception {
        var effectivePid = trimToNull(row.getValue(PID));
        if (effectivePid == null) {
            effectivePid = trimToNull(pid);
        }
        if (effectivePid == null) {
            return BatchProcessor.Result.failed("Missing pid");
        }

        log.info("Updating registration metadata for dataset {}", effectivePid);
        getDatasetApi(effectivePid).updateRegistrationMetadata();
        return BatchProcessor.Result.ok("Registration metadata updated");
    }

    private DatasetApi getDatasetApi(String datasetId) {
        try {
            return dataverseClient.dataset(Integer.parseInt(datasetId));
        }
        catch (NumberFormatException e) {
            return dataverseClient.dataset(datasetId);
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private String trimToNull(String value) {
        return isBlank(value) ? null : value.trim();
    }
}
