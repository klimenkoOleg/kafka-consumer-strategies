
**There are different Kafka consumer cases considered, each in different branch.**

# 1. Branch **case-1-simple-after**
Fast and simple implementation of 
![image](https://user-images.githubusercontent.com/37443840/113258476-a68e5580-92d4-11eb-982f-f300aeae7196.png)


# 2. Branch **case-2-error-handler-after**
Consumer configuration was moved from config to Java code, error handling was added for message formatting error.

![image](https://user-images.githubusercontent.com/37443840/113259358-b490a600-92d5-11eb-8fb3-780510dabf7a.png)


# 3. Branch **case-3-spring-retry-after**

Retry operation has been implemented using built in Spring libraries.
![image](https://user-images.githubusercontent.com/37443840/113260846-6b415600-92d7-11eb-99a0-b43ebb3187eb.png)
![image](https://user-images.githubusercontent.com/37443840/113264631-dbea7180-92db-11eb-9d64-abd6d972736d.png)


# 4. Branch **case-4-spring-SeekToCurrentErrorHandler**

Similar to Case 3 but used latest Spring Boot 2.3 class SeekToCurrentErrorHandler
![image](https://user-images.githubusercontent.com/37443840/113264705-eefd4180-92db-11eb-9320-463a07ee6f9f.png)



# 5. Branch **case-5-different-queues-with-delay-2**

![image](https://user-images.githubusercontent.com/37443840/113265892-510a7680-92dd-11eb-8062-c4a4b2dae677.png)




List of used sources:

* https://stackoverflow.com/questions/57722688/is-there-a-way-to-get-the-last-message-from-kafka-topic
* https://stackoverflow.com/questions/58821537/spring-kafka-offset-increment-even-auto-commit-offset-is-set-to-false
* https://maestralsolutions.com/spring-boot-implementation-for-apache-kafka-with-kafka-tool/
* https://docs.spring.io/spring-kafka/api/org/springframework/kafka/listener/adapter/RetryingMessageListenerAdapter.html
* https://stackoverflow.com/questions/38406298/why-cant-i-increase-session-timeout-ms
* https://medium.com/trendyol-tech/how-to-implement-retry-logic-with-spring-kafka-710b51501ce2
* https://habr.com/ru/company/southbridge/blog/531838/
* https://habr.com/ru/company/tinkoff/blog/487094/
* https://stackoverflow.com/questions/60172304/how-to-retry-with-spring-kafka-version-2-2/60173862#60173862
* https://stackoverflow.com/questions/60373910/spring-kafka-consume-records-with-some-delay
* https://medium.com/@shanaka.fernando2/spring-kafka-re-try-with-spring-retry-8e064483d56f
* https://stackoverflow.com/questions/51205932/is-it-possible-to-set-groupid-in-spring-boot-stream-kafka-at-startup-or-compile
* https://stackoverflow.com/questions/60373910/spring-kafka-consume-records-with-some-delay
* https://medium.com/trendyol-tech/how-to-implement-retry-logic-with-spring-kafka-710b51501ce2 !
* https://habr.com/ru/company/southbridge/blog/531838/ !
* https://habr.com/ru/company/tinkoff/blog/487094/ !
* https://medium.com/naukri-engineering/retry-mechanism-and-delay-queues-in-apache-kafka-528a6524f722 !
* https://blog.pragmatists.com/retrying-consumer-architecture-in-the-apache-kafka-939ac4cb851a !
* https://medium.com/@shanaka.fernando2/spring-kafka-re-try-with-spring-retry-8e064483d56f !
* https://medium.com/trendyol-tech/how-to-implement-retry-logic-with-spring-kafka-710b51501ce2 !!
* https://www.confluent.io/blog/spring-kafka-can-your-kafka-consumers-handle-a-poison-pill/ 
