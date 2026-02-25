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
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.model.user.BuiltinUser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Command(name = "users-import",
         description = "Import users from a CSV file into Dataverse",
         mixinStandardHelpOptions = true)
@RequiredArgsConstructor
public class UsersImport extends AbstractDatabaseCmd {
    @Option(names = { "-i", "--input-csv" }, required = true, description = "CSV file containing users and initial passwords")
    private String inputCsv;

    @Option(names = { "-k", "--builtin-users-key" }, required = true, description = ":BuiltinUsersKey set in Dataverse (BuiltinUsers.KEY up to v6.8)")
    private String builtinUsersKey;

    @Option(names = { "--dry-run" }, description = "Print actions without modifying Dataverse or database")
    private boolean dryRun;

    private final DataverseClient dataverseClient;

    @Override
    protected Integer doCall() throws Exception {
        log.info("Importing users from CSV: {}", inputCsv);
        List<Pair<BuiltinUser, String>> users = readUsersFromCsv(inputCsv);
        for (var pair : users) {
            var user = pair.getLeft();
            var password = pair.getRight();
            if (dryRun) {
                log.warn("Dry-run: would create user {}", user.getUserName());
                continue;
            }
            var response = dataverseClient.builtinUsers(builtinUsersKey)
                .create(user, password);
            if (response != null && "OK".equalsIgnoreCase(response.getEnvelope().getStatus())) {
                log.info("Imported user {}", user.getUserName());
            }
            else {
                log.error("Error creating user {}: {}", user.getUserName(), response != null ? response.getEnvelope().getMessage() : "null");
            }
        }
        return 0;
    }

    private List<Pair<BuiltinUser, String>> readUsersFromCsv(String csvFile) throws Exception {
        List<Pair<BuiltinUser, String>> users = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile));
            CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get().parse(reader)) {
            for (CSVRecord record : parser) {
                var user = new BuiltinUser();
                user.setUserName(record.get("Username"));
                user.setFirstName(record.get("GivenName"));
                user.setLastName(record.get("FamilyName"));
                user.setEmail(record.get("Email"));
                user.setAffiliation(record.get("Affiliation"));
                user.setPosition(record.isMapped("Position") ? record.get("Position") : null);
                String password = record.get("Password");
                users.add(Pair.of(user, password));
            }
        }
        return users;
    }
}
