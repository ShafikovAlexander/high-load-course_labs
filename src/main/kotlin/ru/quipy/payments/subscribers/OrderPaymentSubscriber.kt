package ru.quipy.payments.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.ExternalServicesConfig
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import ru.quipy.payments.logic.create
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.*
import java.util.concurrent.Executors
import javax.annotation.PostConstruct
import ru.quipy.payments.config.AccountRequestsInfo
import ru.quipy.payments.logic.*
import java.util.concurrent.locks.ReentrantLock

@Service
class OrderPaymentSubscriber {

    val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    //@Autowired
    //@Qualifier(ExternalServicesConfig.PRIMARY_PAYMENT_BEAN)
    //private lateinit var paymentService: PaymentService

    private val paymentExecutor = Executors.newFixedThreadPool(300, NamedThreadFactory("payment-executor"))

    private val accounts: List<AccountRequestsInfo> = mutableListOf(
            AccountRequestsInfo(ExternalServicesConfig.accountProps_1),
            AccountRequestsInfo(ExternalServicesConfig.accountProps_2)
    )

    private val mutex = ReentrantLock()

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(
                OrderAggregate::class,
                "payments:order-subscriber",
                retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(OrderPaymentStartedEvent::class) { event ->
                paymentExecutor.submit {
                    val createdEvent = paymentESService.create {
                        it.create(
                            event.paymentId,
                            event.orderId,
                            event.amount,
                        )
                    }
                    logger.warn("Payment ${createdEvent.paymentId} for order ${event.orderId} created.")

                    var accountInfo: AccountRequestsInfo

                    while (true) {
                        val currTime = System.currentTimeMillis()
                        mutex.lock()
                        accountInfo =
                                accounts.maxByOrNull { if (it.getPendingRequestsAmount() < it.getParallelRequests() && it.getLastSecondRequestsAmount() < it.getRateLimitPerSec() && (currTime + it.getAverageDuration() - event.createdAt) < 80_000) it.getPriority() else 0 }!!

                        if (currTime + accountInfo.getAverageDuration() - event.createdAt >= 80_000) {
                            logger.warn("LOG  ${currTime + accountInfo.getAverageDuration() - event.createdAt >= 80_000}")
                            return@submit
                        }
                        if (accountInfo.getPendingRequestsAmount() < accountInfo.getParallelRequests() && accountInfo.getLastSecondRequestsAmount() < accountInfo.getRateLimitPerSec()) {
                            break
                        } else {
                            mutex.unlock()
                            Thread.sleep(75)
                            continue
                        }

                    }
                    logger.warn("[LOG 1] PendingRequestsAmount: ${accountInfo.getPendingRequestsAmount()}, ParallelRequests: ${accountInfo.getParallelRequests()}")
                    logger.warn("[LOG 2] LastSecondRequestsAmount: ${accountInfo.getLastSecondRequestsAmount()}, RateLimitPerSec: ${accountInfo.getRateLimitPerSec()}")

                    logger.warn("[LOG 3] AccountName: ${accountInfo.getExternalServiceProperties().accountName}, AccountPriority: ${accountInfo.getExternalServiceProperties().priority}")

                    accountInfo.addTimestamp()
                    accountInfo.incrementPendingRequestsAmount()

                    mutex.unlock()
                    val paymentService = PaymentExternalServiceImpl(accountInfo, mutex, paymentESService)

                    paymentService.submitPaymentRequest(createdEvent.paymentId, event.amount, event.createdAt)
                }
            }
        }
    }
}