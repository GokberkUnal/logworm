package com.gokgor.logworm.consumergroup;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/consumer-groups")
@RequiredArgsConstructor
public class ConsumerGroupController {

    private final ConsumerGroupService consumerGroupService;

    @GetMapping
    public List<ConsumerGroupSummary> listGroups() {
        return consumerGroupService.listGroups();
    }

    @GetMapping("/{id}")
    public ConsumerGroupDetail getGroup(@PathVariable String id) {
        return consumerGroupService.getGroup(id);
    }
}
