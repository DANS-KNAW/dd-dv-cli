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
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.model.dataset.UpdateType;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;
import java.util.stream.Stream;

@Command(
    name = "dataset-publish",
    description = "Publishes the draft version of a dataset; does not wait for the publication to finish",
    mixinStandardHelpOptions = true
)
@Slf4j
public class DatasetPublish implements Callable<Integer> {
    private final DataverseClient dataverseClient;

    @Option(names = { "--major" }, description = "Publish as a major version")
    private boolean major;

    @Option(names = { "--minor" }, description = "Publish as a minor version (default)")
    private boolean minor;

    @Option(names = { "--updatecurrent" }, description = "Update the current version instead of creating a new one (requires superuser)")
    private boolean updateCurrent;

    @Option(names = { "--versionNote" }, description = "Version note to set on the draft before publication")
    private String versionNote;

    @Parameters(index = "0", paramLabel = "databaseIdOrPid", description = "Database id (digits only) or persistent identifier (including protocol, e.g. doi:10.5072/FK2/ABC) of the dataset")
    private String datasetId;

    public DatasetPublish(DataverseClient dataverseClient) {
        this.dataverseClient = dataverseClient;
    }

    @Override
    public Integer call() {
        try {
            if (Stream.of(major, minor, updateCurrent).filter(b -> b).count() > 1) {
                System.err.println("--major, --minor and --updatecurrent are mutually exclusive");
                return 1;
            }
            var updateType = major ? UpdateType.major : updateCurrent ? UpdateType.updatecurrent : UpdateType.minor;
            var datasetApi = datasetId.matches("\\d+") ? dataverseClient.dataset(Integer.parseInt(datasetId)) : dataverseClient.dataset(datasetId);
            if (versionNote != null) {
                log.info("Setting version note on dataset {}", datasetId);
                datasetApi.setVersionNote(versionNote);
            }
            log.info("Publishing dataset {} ({})", datasetId, updateType);
            datasetApi.publish(updateType, false);
            System.err.println("Publication of dataset " + datasetId + " requested (" + updateType + ")");
            return 0;
        }
        catch (Exception e) {
            log.error("Error publishing dataset", e);
            System.err.println("Error publishing dataset: " + e.getMessage());
            return 1;
        }
    }
}
