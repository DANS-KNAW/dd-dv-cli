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

import nl.knaw.dans.lib.dataverse.AdminApi;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.DataverseHttpResponse;
import nl.knaw.dans.lib.dataverse.model.DataMessage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SettingsCommandsTest {

    @Test
    void settings_get_calls_get_setting_endpoint() throws Exception {
        var api = Mockito.mock(AdminApi.class);
        var response = mockDataMessageResponse();
        Mockito.when(api.getDatabaseSetting("DataverseSiteUrl")).thenReturn(response);

        var exitCode = new CommandLine(new SettingsGet(api)).execute("DataverseSiteUrl");

        assertThat(exitCode).isZero();
        Mockito.verify(api).getDatabaseSetting("DataverseSiteUrl");
    }

    @Test
    void settings_get_returns_non_zero_when_get_setting_fails() throws Exception {
        var api = Mockito.mock(AdminApi.class);
        Mockito.when(api.getDatabaseSetting("DataverseSiteUrl")).thenThrow(new DataverseException(500, "failure"));

        var exitCode = new CommandLine(new SettingsGet(api)).execute("DataverseSiteUrl");

        assertThat(exitCode).isEqualTo(1);
        Mockito.verify(api).getDatabaseSetting("DataverseSiteUrl");
    }

    @Test
    void settings_put_calls_put_setting_endpoint() throws Exception {
        var api = Mockito.mock(AdminApi.class);
        var response = mockMapResponse();
        Mockito.when(api.putDatabaseSetting("DataverseSiteUrl", "https://example.org")).thenReturn(response);

        var exitCode = new CommandLine(new SettingsPut(api)).execute("DataverseSiteUrl", "https://example.org");

        assertThat(exitCode).isZero();
        Mockito.verify(api).putDatabaseSetting("DataverseSiteUrl", "https://example.org");
    }

    @Test
    void settings_put_returns_non_zero_when_put_setting_fails() throws Exception {
        var api = Mockito.mock(AdminApi.class);
        Mockito.when(api.putDatabaseSetting("DataverseSiteUrl", "https://example.org")).thenThrow(new DataverseException(500, "failure"));

        var exitCode = new CommandLine(new SettingsPut(api)).execute("DataverseSiteUrl", "https://example.org");

        assertThat(exitCode).isEqualTo(1);
        Mockito.verify(api).putDatabaseSetting("DataverseSiteUrl", "https://example.org");
    }

    @Test
    void settings_list_calls_list_settings_endpoint() throws Exception {
        var api = Mockito.mock(AdminApi.class);
        var response = mockMapResponse();
        Mockito.when(api.listAllDatabaseSettings()).thenReturn(response);

        var exitCode = new CommandLine(new SettingsList(api)).execute();

        assertThat(exitCode).isZero();
        Mockito.verify(api).listAllDatabaseSettings();
    }

    @Test
    void settings_list_returns_non_zero_when_list_settings_fails() throws Exception {
        var api = Mockito.mock(AdminApi.class);
        Mockito.when(api.listAllDatabaseSettings()).thenThrow(new DataverseException(500, "failure"));

        var exitCode = new CommandLine(new SettingsList(api)).execute();

        assertThat(exitCode).isEqualTo(1);
        Mockito.verify(api).listAllDatabaseSettings();
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<DataMessage> mockDataMessageResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<Map<String, String>> mockMapResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }
}
