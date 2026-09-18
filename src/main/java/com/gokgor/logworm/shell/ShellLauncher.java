package com.gokgor.logworm.shell;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.core.ShellRunner;

import lombok.extern.slf4j.Slf4j;

/**
 * Owns the console lifecycle: banner (printed by Spring Boot) → startup wizard → interactive
 * prompt → on {@code exit}/{@code quit} close the context and end the JVM.
 *
 * <p>Spring Shell's own launcher is also an ApplicationRunner, but it leaves the embedded web
 * server and scheduler running after exit. This one runs first (highest precedence), drives the
 * same shell runner and ends the JVM, so Spring Shell's runner never gets its turn.
 */
@InteractiveShellComponent
@Order(Ordered.HIGHEST_PRECEDENCE)
@Slf4j
public class ShellLauncher implements ApplicationRunner {

    private final ShellRunner shellRunner;
    private final StartupWizard wizard;
    private final ConfigurableApplicationContext context;
    public ShellLauncher(@Qualifier("jlineShellRunner") ShellRunner shellRunner, StartupWizard wizard,
            ConfigurableApplicationContext context) {
        this.shellRunner = shellRunner;
        this.wizard = wizard;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (System.console() != null) {
            wizard.ask();
        }
        shellRunner.run(args.getSourceArgs());
        int code = SpringApplication.exit(context);
        System.exit(code);
    }
}
