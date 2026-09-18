package com.gokgor.logworm.shell;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * A bean that only exists when the interactive shell is on. Spring Shell registers its
 * terminal / flow / runner beans under the same condition, so anything depending on them
 * must be guarded the same way (tests run with the shell off).
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Component
@ConditionalOnProperty(name = "spring.shell.interactive.enabled", havingValue = "true", matchIfMissing = true)
public @interface InteractiveShellComponent {
}
