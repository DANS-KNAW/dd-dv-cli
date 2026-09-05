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

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.WorkflowsApi;
import nl.knaw.dans.lib.dataverse.model.workflow.Workflow;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(name = "workflow-add",
         description = "Adds a workflow from a JSON definition",
         mixinStandardHelpOptions = true)
@RequiredArgsConstructor
public class WorkflowAdd implements Callable<Integer> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final WorkflowsApi workflowsApi;

    @Option(names = { "-i", "--input-json" }, required = true, description = "Path to workflow JSON")
    private Path inputJson;

    @Override
    public Integer call() throws Exception {
        try {
            var workflow = MAPPER.readValue(inputJson.toFile(), Workflow.class);
            var response = workflowsApi.addWorkflow(workflow);
            System.out.println(response.getEnvelopeAsString());
            return 0;
        }
        catch (DataverseException e) {
            System.err.println("Error adding workflow: " + e.getMessage());
            return 1;
        }
    }
}
