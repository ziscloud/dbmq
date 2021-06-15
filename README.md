# DBMQ

DBMQ is a lite message queue that use database as broker, it based on Apache RocketMQ which is a distributed messaging
and streaming platform.

# Why we create it

In our circumstance, we need an intra-application message processing for online request handling, e.g.

* we need to send an SMS to customer
* we need to push data to another application, but before that, we have to query DB , in some situation it could be more
  than once , or query another application to assemble data for the target application.
* we need to call the API/service provided by another application, but it has low capacity, we need a queue to cache the
  pending request.
* ...

We have following constraints:

* we do not want these tasks involved in our main processing, we want to response to the client ASAP.
* we must make sure these tasks complete successfully at least once.
* we do not want to employee RocketMQ, RabbitMQ, or Kafka, these tools are good, but they need more resources and
  maintenance, it is not affordable for us.

We need a lite weighted MQ that can be embedded into application, and we already got JAVA and database/redis, so we
build it. 