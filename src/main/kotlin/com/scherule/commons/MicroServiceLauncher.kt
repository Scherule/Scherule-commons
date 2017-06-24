package com.scherule.commons

import io.vertx.core.DeploymentOptions
import io.vertx.core.Launcher
import io.vertx.core.VertxOptions
import io.vertx.core.json.DecodeException
import io.vertx.core.json.JsonObject

import java.io.File
import java.io.FileNotFoundException
import java.util.Scanner

class MicroServiceLauncher : Launcher() {

    companion object {
        fun main(args: Array<String>) {
            MicroServiceLauncher().dispatch(args)
        }
    }

    override fun beforeStartingVertx(options: VertxOptions?) {
        options!!.setClustered(true).clusterHost = "127.0.0.1"
    }

    override fun beforeDeployingVerticle(deploymentOptions: DeploymentOptions?) {
        super.beforeDeployingVerticle(deploymentOptions)

        if (deploymentOptions!!.config == null) {
            deploymentOptions.config = JsonObject()
        }

        val conf = File("src/conf/config.json")
        deploymentOptions.config.mergeIn(getConfiguration(conf))
    }

    private fun getConfiguration(config: File): JsonObject {
        var conf = JsonObject()
        if (config.isFile) {
            println("Reading config file: " + config.absolutePath)
            try {
                Scanner(config).useDelimiter("\\A").use { scanner ->
                    val sconf = scanner.next()
                    try {
                        conf = JsonObject(sconf)
                    } catch (e: DecodeException) {
                        System.err.println("Configuration file $sconf does not contain a valid JSON object")
                    }
                }
            } catch (e: FileNotFoundException) {
                // Ignore it.
            }

        } else {
            println("Config file not found " + config.absolutePath)
        }
        return conf
    }

}
