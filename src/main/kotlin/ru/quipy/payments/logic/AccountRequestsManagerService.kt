package ru.quipy.payments.logic

import ru.quipy.payments.config.AccountRequestsInfo
import java.util.*
import java.util.concurrent.Executors

class AccountRequestsManagerService(
        private val properties: List<AccountRequestsInfo>,
) {
    private val priorityQueue = PriorityQueue<AccountRequestsInfo>(compareBy {
        if (it.getPendingRequestsAmount() <= it.getParallelRequests() && it.getLastSecondRequestsAmount() <= it.getRateLimitPerSec()) it.getPriority() else 0
    })
    private val threadPool = Executors.newFixedThreadPool(20)

    fun threadPoolExecute(){

    }

}