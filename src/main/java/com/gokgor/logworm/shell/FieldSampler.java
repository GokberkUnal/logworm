package com.gokgor.logworm.shell;

import java.util.ArrayList;
import java.util.List;

import com.gokgor.logworm.message.MessageQuery;
import com.gokgor.logworm.message.MessageService;
import com.gokgor.logworm.message.ValueFormat;

import lombok.RequiredArgsConstructor;

/** Discovers JSON field names from a topic's most recent messages (used by the wizard steps). */
@InteractiveShellComponent
@RequiredArgsConstructor
public class FieldSampler {

    static final int SAMPLE = 50;

    private final MessageService messageService;

    /** Field paths seen in the last {@value SAMPLE} messages; empty when the topic has no JSON. */
    public List<String> sample(String topic) {
        var page = messageService.read(topic, new MessageQuery(null, null, SAMPLE, null, null, null, ValueFormat.AUTO));
        return MessageFormatter.discoverFields(page.messages());
    }

    /** Same, with {@code key} first so rules can target the record key. */
    public List<String> sampleWithKey(String topic) {
        List<String> fields = new ArrayList<>();
        fields.add(Rule.KEY_FIELD);
        fields.addAll(sample(topic));
        return fields;
    }
}
