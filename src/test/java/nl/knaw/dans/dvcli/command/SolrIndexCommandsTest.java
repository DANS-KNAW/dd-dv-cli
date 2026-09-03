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
import nl.knaw.dans.lib.dataverse.SolrIndexApi;
import nl.knaw.dans.lib.dataverse.model.DataMessage;
import nl.knaw.dans.lib.dataverse.model.DataMessageSolrIndex;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import picocli.CommandLine;

import static org.assertj.core.api.Assertions.assertThat;

public class SolrIndexCommandsTest {

    @Test
    void index_status_calls_status_endpoint() throws Exception {
        var api = Mockito.mock(SolrIndexApi.class);
        var response = mockResponse();
        Mockito.when(api.status()).thenReturn(response);

        var exitCode = new CommandLine(new IndexStatus(api)).execute();

        assertThat(exitCode).isZero();
        Mockito.verify(api).status();
    }

    @Test
    void index_clear_calls_clear_endpoint() throws Exception {
        var api = Mockito.mock(SolrIndexApi.class);
        var response = mockIndexResponse();
        Mockito.when(api.clear()).thenReturn(response);

        var exitCode = new CommandLine(new IndexClear(api)).execute();

        assertThat(exitCode).isZero();
        Mockito.verify(api).clear();
    }

    @Test
    void index_all_calls_index_all_endpoint() throws Exception {
        var api = Mockito.mock(SolrIndexApi.class);
        var response = mockIndexResponse();
        Mockito.when(api.indexAll()).thenReturn(response);

        var exitCode = new CommandLine(new IndexAll(api)).execute();

        assertThat(exitCode).isZero();
        Mockito.verify(api).indexAll();
    }

    @Test
    void index_dataset_uses_numeric_dataset_id_when_argument_is_an_integer() throws Exception {
        var api = Mockito.mock(SolrIndexApi.class);
        var response = mockIndexResponse();
        Mockito.when(api.indexDataset(42)).thenReturn(response);

        var exitCode = new CommandLine(new IndexDataset(api)).execute("42");

        assertThat(exitCode).isZero();
        Mockito.verify(api).indexDataset(42);
        Mockito.verify(api, Mockito.never()).indexDataset("42");
    }

    @Test
    void index_dataset_uses_persistent_identifier_when_argument_is_not_an_integer() throws Exception {
        var api = Mockito.mock(SolrIndexApi.class);
        var response = mockIndexResponse();
        Mockito.when(api.indexDataset("doi:10.5072/FK2/ABCDEF")).thenReturn(response);

        var exitCode = new CommandLine(new IndexDataset(api)).execute("doi:10.5072/FK2/ABCDEF");

        assertThat(exitCode).isZero();
        Mockito.verify(api).indexDataset("doi:10.5072/FK2/ABCDEF");
    }

    @Test
    void index_dataverse_calls_index_dataverse_endpoint() throws Exception {
        var api = Mockito.mock(SolrIndexApi.class);
        var response = mockResponse();
        Mockito.when(api.indexDataverse(7)).thenReturn(response);

        var exitCode = new CommandLine(new IndexDataverse(api)).execute("7");

        assertThat(exitCode).isZero();
        Mockito.verify(api).indexDataverse(7);
    }

    @Test
    void index_clear_timestamps_calls_clear_timestamps_endpoint() throws Exception {
        var api = Mockito.mock(SolrIndexApi.class);
        var response = mockResponse();
        Mockito.when(api.clearTimestamps()).thenReturn(response);

        var exitCode = new CommandLine(new IndexClearTimestamps(api)).execute();

        assertThat(exitCode).isZero();
        Mockito.verify(api).clearTimestamps();
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<DataMessage> mockResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<DataMessageSolrIndex> mockIndexResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }
}
