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
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(
    name = "dataset-set-versionNote",
    description = "Sets the version note of a dataset version",
    mixinStandardHelpOptions = true
)
public class DatasetSetVersionNote extends AbstractDatasetCmd implements Callable<Integer> {

    @Option(names = { "--version" }, paramLabel = "<version>", defaultValue = ":draft",
            description = "Version number (e.g. 1.0) or a tag such as :draft, :latest or :latest-published (default: ${DEFAULT-VALUE})")
    private String version;

    @Parameters(index = "1", paramLabel = "versionNote", description = "The version note to set")
    private String versionNote;

    public DatasetSetVersionNote(DataverseClient dataverseClient) {
        super(dataverseClient);
    }

    @Override
    public Integer call() {
        try {
            getDatasetApi().setVersionNote(version, versionNote);
            System.err.println("Version note set on version " + version + " of dataset " + pid);
            return 0;
        }
        catch (Exception e) {
            System.err.println("Error setting version note: " + e.getMessage());
            return 1;
        }
    }
}
