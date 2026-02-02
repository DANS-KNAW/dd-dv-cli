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

package nl.knaw.dans.dvcli;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.dvcli.command.BannerList;
import nl.knaw.dans.lib.util.PicocliVersionProvider;
import nl.knaw.dans.dvcli.config.DdDvCliConfig;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "dv",
         mixinStandardHelpOptions = true,
         versionProvider = PicocliVersionProvider.class,
         description = "Dataverse command-line interface")
@Slf4j
public class DdDvCli extends nl.knaw.dans.lib.util.AbstractCommandLineApp<DdDvCliConfig> {
    public static void main(String[] args) throws Exception {
        new DdDvCli().run(args);
    }

    public String getName() {
        return "Dataverse command-line interface";
    }

    @Override
    public void configureCommandLine(CommandLine commandLine, DdDvCliConfig config) {
        // Build the Dataverse API client using the same approach as dd-dataverse-cli
        log.debug("Building Dataverse client");
        var dataverseClient = config.getApi().build();

        log.debug("Configuring command line for dv commands");
        commandLine.addSubcommand(new BannerList(dataverseClient.admin()));
    }
}
