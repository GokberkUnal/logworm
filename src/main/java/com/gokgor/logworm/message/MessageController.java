package com.gokgor.logworm.message;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/topics/{name}/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageService messageService;

    /**
     * Examples:
     * <pre>
     * GET /api/topics/demo-logs/messages                         newest 100 across all partitions
     * GET /api/topics/demo-logs/messages?partition=0&limit=20    newest 20 in partition 0
     * GET /api/topics/demo-logs/messages?partition=0&offset=500  100 records from offset 500
     * GET /api/topics/demo-logs/messages?value=ERROR&header=trace-id
     * </pre>
     */
    @GetMapping
    public MessagePage read(
            @PathVariable String name,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) Long offset,
            @RequestParam(defaultValue = "" + MessageQuery.DEFAULT_LIMIT) int limit,
            @RequestParam(required = false) String key,
            @RequestParam(required = false) String value,
            @RequestParam(required = false) String header,
            @RequestParam(defaultValue = "AUTO") ValueFormat format) {
        return messageService.read(name, new MessageQuery(partition, offset, limit, key, value, header, format));
    }
}
