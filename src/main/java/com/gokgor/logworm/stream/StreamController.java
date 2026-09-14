package com.gokgor.logworm.stream;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.gokgor.logworm.message.ValueFormat;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/topics/{name}/stream")
@RequiredArgsConstructor
public class StreamController {

    private final LiveTailService liveTailService;
    private final StreamProperties properties;

    /**
     * Server-Sent Events. Events: {@code connected} (once), {@code message} (each record),
     * {@code stats} (when records were dropped/skipped), plus keep-alive comments.
     * <pre>
     * curl -N 'localhost:8080/api/topics/demo-logs/stream?value=ERROR&rate=50'
     * </pre>
     */
    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(
            @PathVariable String name,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) String key,
            @RequestParam(required = false) String value,
            @RequestParam(required = false) String header,
            @RequestParam(defaultValue = "AUTO") ValueFormat format,
            @RequestParam(required = false) Integer rate) {
        int effectiveRate = Math.min(rate != null ? rate : properties.defaultRate(), properties.maxRate());
        return liveTailService.tail(name, new StreamQuery(partition, key, value, header, format, effectiveRate));
    }
}
