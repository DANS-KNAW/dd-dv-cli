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
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class WorkflowCommandsTest {

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
        var response = mockResponse();
        Mockito.when(api.getWorkflow(42L)).thenReturn(response);

        var exitCode = new CommandLine(new WorkflowGet(api)).execute("42");

        assertThat(exitCode).isZero();
        Mockito.verify(api).getWorkflow(42L);
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
    void workflow_get_ip_whitelist_returns_non_zero_on_error() throws Exception {
        var api = Mockito.mock(WorkflowsApi.class);
        Mockito.when(api.getIpWhitelist()).thenThrow(new DataverseException(500, "failure"));

        var exitCode = new CommandLine(new WorkflowGetIpWhitelist(api)).execute();

        assertThat(exitCode).isEqualTo(1);
        Mockito.verify(api).getIpWhitelist();
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<Object> mockResponse() {
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
}
