package com.gokgor.logworm.kafka;

import java.lang.reflect.Proxy;

import org.apache.kafka.clients.admin.Admin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Exposes the current AdminClient as an {@link Admin} bean. It is a thin proxy over
 * {@link KafkaConnection#admin()}, so a runtime reconnect is transparent to every service.
 * {@code close()} is a no-op here: the connection owns the client's lifecycle.
 */
@Configuration
public class KafkaAdminConfig {

    @Bean
    public Admin adminClient(KafkaConnection connection) {
        return (Admin) Proxy.newProxyInstance(
                Admin.class.getClassLoader(),
                new Class<?>[] {Admin.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    try {
                        return method.invoke(connection.admin(), args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
