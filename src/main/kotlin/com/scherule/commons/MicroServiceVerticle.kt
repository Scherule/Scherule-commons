package com.scherule.commons

import io.vertx.core.*
import io.vertx.core.impl.ConcurrentHashSet
import io.vertx.rx.java.RxHelper
import io.vertx.rxjava.core.Vertx
import io.vertx.servicediscovery.Record
import io.vertx.servicediscovery.ServiceDiscovery
import io.vertx.servicediscovery.ServiceDiscoveryOptions
import io.vertx.servicediscovery.types.EventBusService
import io.vertx.servicediscovery.types.HttpEndpoint
import io.vertx.servicediscovery.types.MessageSource
import rx.Single
import java.util.*

open class MicroServiceVerticle : AbstractVerticle() {

    protected lateinit var rxVertx: Vertx
    private lateinit var discovery: ServiceDiscovery
    private val registeredRecords: MutableSet<Record> = ConcurrentHashSet()

    override fun start(startFuture: Future<Void>) {
        super.start(startFuture)
        start()
    }

    override fun start() {
        rxVertx = Vertx.newInstance(vertx)
        discovery = ServiceDiscovery.create(vertx, ServiceDiscoveryOptions().setBackendConfiguration(config()))
    }

    fun publishHttpEndpoint(name: String, host: String, port: Int, completionHandler: Handler<AsyncResult<Void>>) {
        val record = HttpEndpoint.createRecord(name, host, port, "/")
        publish(record, completionHandler)
    }

    fun publishMessageSource(name: String, address: String, contentClass: Class<*>, completionHandler: Handler<AsyncResult<Void>>) {
        val record = MessageSource.createRecord(name, address, contentClass)
        publish(record, completionHandler)
    }

    fun publishMessageSource(name: String, address: String, completionHandler: Handler<AsyncResult<Void>>) {
        val record = MessageSource.createRecord(name, address)
        publish(record, completionHandler)
    }

    fun publishEventBusService(name: String, address: String, serviceClass: Class<*>, completionHandler: Handler<AsyncResult<Void>>) {
        val record = EventBusService.createRecord(name, address, serviceClass)
        publish(record, completionHandler)
    }


    fun rxPublishHttpEndpoint(name: String, host: String, port: Int): Single<Void> {
        val record = io.vertx.rxjava.servicediscovery.types.HttpEndpoint.createRecord(name, host, port, "/")
        return rxPublish(record)
    }

    fun rxPublishMessageSource(name: String, address: String): Single<Void> {
        val record = io.vertx.rxjava.servicediscovery.types.MessageSource.createRecord(name, address)
        return rxPublish(record)
    }

    private fun rxPublish(record: Record): Single<Void> {
        val adapter = RxHelper.observableFuture<Void>()
        publish(record, adapter.toHandler())
        return adapter.toSingle()
    }

    private fun publish(record: Record, completionHandler: Handler<AsyncResult<Void>>) {
        discovery.publish(record) { ar ->
            if (ar.succeeded()) {
                registeredRecords.add(record)
            }
            completionHandler.handle(ar.map<Void>(null as Void?))
        }
    }

    @Throws(Exception::class)
    override fun stop(future: Future<Void>) {
        val futures = ArrayList<Future<*>>()
        for (record in registeredRecords) {
            val deregisterFuture = Future.future<Void>()
            futures.add(deregisterFuture)
            discovery.unpublish(record.registration, deregisterFuture)
        }

        if (futures.isEmpty()) {
            discovery.close()
            future.complete()
        } else {
            val composite = CompositeFuture.all(futures)
            composite.setHandler { ar ->
                discovery.close()
                if (ar.failed()) {
                    future.fail(ar.cause())
                } else {
                    future.complete()
                }
            }
        }
    }

}
