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

import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.DataverseHttpResponse;
import nl.knaw.dans.lib.dataverse.WorkflowsApi;
import nl.knaw.dans.lib.dataverse.model.DataMessage;
import nl.knaw.dans.lib.dataverse.model.workflow.Workflow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class WorkflowCommandsTest {

    @TempDir
    Path tempDir;

    @Test
    void workflow_add_calls_add_endpoint() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        var response = mockWorkflowResponse();
        var inputJson = tempDir.resolve("workflow.json");
        Files.writeString(inputJson, """
            {
              "name": "Test workflow",
              "steps": []
            }
            """);
        Mockito.when(api.addWorkflow(ArgumentMatchers.any(Workflow.class))).thenReturn(response);

        var exitCode = new CommandLine(new WorkflowAdd(api)).execute("--input-json", inputJson.toString());

        assertThat(exitCode).isZero();
        Mockito.verify(api).addWorkflow(ArgumentMatchers.any(Workflow.class));
    }

    @Test
    void workflow_list_calls_list_endpoint() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        var response = mockResponse();
        Mockito.when(api.listWorkflows()).thenReturn(response);

        var exitCode = new CommandLine(new WorkflowList(api)).execute();

        assertThat(exitCode).isZero();
        Mockito.verify(api).listWorkflows();
    }

    @Test
    void workflow_get_calls_get_endpoint() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        var response = mockWorkflowResponse();
        Mockito.when(api.getWorkflow(42L)).thenReturn(response);

        var exitCode = new CommandLine(new WorkflowGet(api)).execute("42");

        assertThat(exitCode).isZero();
        Mockito.verify(api).getWorkflow(42L);
    }

    @Test
    void workflow_delete_calls_delete_endpoint() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        var response = mockDataMessageResponse();
        Mockito.when(api.deleteWorkflow(42L)).thenReturn(response);

        var exitCode = new CommandLine(new WorkflowDelete(api)).execute("42");

        assertThat(exitCode).isZero();
        Mockito.verify(api).deleteWorkflow(42L);
    }

    @Test
    void workflow_list_defaults_calls_endpoint() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        var response = mockMapResponse();
        Mockito.when(api.listDefaults()).thenReturn(response);

        var exitCode = new CommandLine(new WorkflowListDefaults(api)).execute();

        assertThat(exitCode).isZero();
        Mockito.verify(api).listDefaults();
    }

    @Test
    void workflow_get_default_calls_endpoint() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        var response = mockWorkflowResponse();
        Mockito.when(api.getDefault("PrePublishDataset")).thenReturn(response);

        var exitCode = new CommandLine(new WorkflowGetDefault(api)).execute("PrePublishDataset");

        assertThat(exitCode).isZero();
        Mockito.verify(api).getDefault("PrePublishDataset");
    }

    @Test
    void workflow_set_default_calls_endpoint() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        var response = mockDataMessageResponse();
        Mockito.when(api.setDefault("PrePublishDataset", 42L)).thenReturn(response);

        var exitCode = new CommandLine(new WorkflowSetDefault(api)).execute("PrePublishDataset", "42");

        assertThat(exitCode).isZero();
        Mockito.verify(api).setDefault("PrePublishDataset", 42L);
    }

    @Test
    void workflow_delete_default_calls_endpoint() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        var response = mockDataMessageResponse();
        Mockito.when(api.deleteDefault("PrePublishDataset")).thenReturn(response);

        var exitCode = new CommandLine(new WorkflowDeleteDefault(api)).execute("PrePublishDataset");

        assertThat(exitCode).isZero();
        Mockito.verify(api).deleteDefault("PrePublishDataset");
    }

    @Test
    void workflow_get_ip_whitelist_returns_non_zero_on_error() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        Mockito.when(api.getIpWhitelist()).thenThrow(new DataverseException(500, "failure"));

        var exitCode = new CommandLine(new WorkflowGetIpWhitelist(api)).execute();

        assertThat(exitCode).isEqualTo(1);
        Mockito.verify(api).getIpWhitelist();
    }

    @Test
    void workflow_set_ip_whitelist_calls_endpoint() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        var response = mockStringResponse();
        Mockito.when(api.setIpWhitelist("127.0.0.1/32,10.0.0.0/24")).thenReturn(response);

        var exitCode = new CommandLine(new WorkflowSetIpWhitelist(api)).execute("127.0.0.1/32,10.0.0.0/24");

        assertThat(exitCode).isZero();
        Mockito.verify(api).setIpWhitelist("127.0.0.1/32,10.0.0.0/24");
    }

    @Test
    void workflow_delete_ip_whitelist_calls_endpoint() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        var response = mockDataMessageResponse();
        Mockito.when(api.deleteIpWhitelist()).thenReturn(response);

        var exitCode = new CommandLine(new WorkflowDeleteIpWhitelist(api)).execute();

        assertThat(exitCode).isZero();
        Mockito.verify(api).deleteIpWhitelist();
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<Object> mockResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<Workflow> mockWorkflowResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<DataMessage> mockDataMessageResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<Map> mockMapResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<String> mockStringResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }
}
