package com.gokgor.logworm.topic;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/topics")
@RequiredArgsConstructor
public class TopicController {

    private final TopicService topicService;

    @GetMapping
    public List<TopicSummary> listTopics() {
        return topicService.listTopics();
    }

    @GetMapping("/{name}")
    public TopicDetail getTopic(@PathVariable String name) {
        return topicService.getTopic(name);
    }
}
