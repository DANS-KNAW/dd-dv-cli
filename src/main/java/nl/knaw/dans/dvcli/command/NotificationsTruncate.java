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
import nl.knaw.dans.lib.dataverse.DatabaseApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(
    name = "notifications-truncate",
    description = "Truncate notifications in the Dataverse database: either for all users or a specific user",
    mixinStandardHelpOptions = true
)
@RequiredArgsConstructor
public class NotificationsTruncate extends AbstractDatabaseCmd implements Callable<Integer> {
    private final DatabaseApi dbApi;

    @Option(names = {"--user"}, description = "User ID to truncate notifications for; if omitted, truncates for all users")
    private Integer userId;

    @Option(names = {"--keep"}, required = true, description = "Number of latest notifications to keep per user")
    private int keep;

    @Override
    public Integer doCall() {
        try {
            int deleted;
            if (userId == null) {
                deleted = dbApi.truncateNotificationsForAllUsers(keep);
                System.out.printf("Truncated notifications for all users; kept %d per user; deleted %d records in total%n", keep, deleted);
            }
            else {
                deleted = dbApi.truncateNotificationsForUser(userId, keep);
                System.out.printf("Truncated notifications for user %d; kept %d; deleted %d records%n", userId, keep, deleted);
            }
            return 0;
        }
        catch (Exception e) {
            System.err.println("Error truncating notifications: " + e.getMessage());
            return 1;
        }
    }
}
