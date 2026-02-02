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
import nl.knaw.dans.lib.dataverse.AdminApi;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.banner.Messages;
import nl.knaw.dans.lib.dataverse.model.banner.MessageText;
import org.jspecify.annotations.NonNull;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "banner-add",
         description = "Adds a banner message to the Dataverse installation",
         mixinStandardHelpOptions = true)
@RequiredArgsConstructor
public class BannerAdd implements Callable<Integer> {
    private final AdminApi adminApi;

    @Option(names = {"-m", "--message"}, required = true, description = "Banner message text; can be specified multiple times with different languages")
    private List<String> messages = new ArrayList<>();

    @Option(names = {"-l", "--lang"}, description = "Language for the corresponding --message entry; defaults to 'en' if not provided")
    private List<String> languages = new ArrayList<>();

    @Option(names = {"--dismissible"}, description = "Whether the banner can be dismissed by the user")
    private boolean dismissibleByUser = true;

    @Override
    public Integer call() throws Exception {
        try {
            if (messages.isEmpty()) {
                System.err.println("No message text provided. Use --message to specify at least one.");
                return 2;
            }
            System.out.println("Languages = " + languages);

            var msgs = getMessages();

            var response = adminApi.addBannerMessage(msgs);
            System.out.println(response.getEnvelopeAsString());
            return 0;
        }
        catch (DataverseException e) {
            System.err.println("Error adding banner message: " + e.getMessage());
            return 1;
        }
    }

    private @NonNull Messages getMessages() {
        var msgTexts = new ArrayList<MessageText>();
        for (int i = 0; i < messages.size(); i++) {
            var text = messages.get(i);
            String lang = (i < languages.size()) ? languages.get(i) : "en";
            var mt = new MessageText();
            mt.setLang(lang);
            mt.setMessage(text);
            msgTexts.add(mt);
        }

        var msgs = new Messages();
        msgs.setMessageTexts(msgTexts);
        msgs.setDismissibleByUser(Boolean.toString(dismissibleByUser));
        return msgs;
    }
}
