package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import java.time.Duration
import java.util.concurrent.locks.ReentrantLock


@Configuration
class ExternalServicesConfig {
    companion object {
        const val PRIMARY_PAYMENT_BEAN = "PRIMARY_PAYMENT_BEAN"

        // Ниже приведены готовые конфигурации нескольких аккаунтов провайдера оплаты.
        // Заметьте, что каждый аккаунт обладает своими характеристиками и стоимостью вызова.

        val accountProps_1 = ExternalServiceProperties(
            // most expensive. Call costs 100
            "test",
            "default-1",
            parallelRequests = 10000,
            rateLimitPerSec = 100,
            request95thPercentileProcessingTime = Duration.ofMillis(1000),
            priority = 1,
        )

        val accountProps_2 = ExternalServiceProperties(
            // Call costs 70
            "test",
            "default-2",
            parallelRequests = 100,
            rateLimitPerSec = 30,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            priority = 2,
        )

        val accountProps_3 = ExternalServiceProperties(
            // Call costs 40
            "test",
            "default-3",
            parallelRequests = 30,
            rateLimitPerSec = 8,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            priority = 3,
        )

        // Call costs 30
        val accountProps_4 = ExternalServiceProperties(
            "test",
            "default-4",
            parallelRequests = 8,
            rateLimitPerSec = 5,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            priority = 4,
        )
    }

    //@Bean(PRIMARY_PAYMENT_BEAN)
    //fun fastExternalService() =
    //    PaymentExternalServiceImpl(
    //        accountProps_4,
    //    )
}