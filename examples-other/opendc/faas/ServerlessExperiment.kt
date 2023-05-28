// Slightly modified https://github.com/atlarge-research/opendc/blob/6d2b140311057e54622fdcd6cf7f8850c370414c/opendc-experiments/opendc-experiments-serverless20/src/main/kotlin/org/opendc/experiments/serverless/ServerlessExperiment.kt
package org.opendc.experiments.serverless

import com.typesafe.config.ConfigFactory
import io.opentelemetry.api.metrics.MeterProvider
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.opendc.experiments.serverless.trace.FunctionTraceWorkload
import org.opendc.experiments.serverless.trace.ServerlessTraceReader
import org.opendc.harness.dsl.Experiment
import org.opendc.harness.dsl.anyOf
import org.opendc.serverless.service.ServerlessService
import org.opendc.serverless.service.autoscaler.FunctionTerminationPolicyFixed
import org.opendc.serverless.service.deployer.FunctionInstance
import org.opendc.serverless.service.router.RandomRoutingPolicy
import org.opendc.serverless.simulator.SimFunctionDeployer
import org.opendc.serverless.simulator.delay.DelayInjector
import org.opendc.simulator.compute.SimMachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.core.runBlockingSimulation
import org.opendc.telemetry.sdk.toOtelClock
import java.io.File
import java.util.*
import kotlin.math.max
import kotlin.system.measureTimeMillis

public class FixedDelayInjector(private val delay: Long) : DelayInjector {
    override fun getColdStartDelay(instance: FunctionInstance): Long {
        return delay
    }
}

public class ServerlessExperiment : Experiment("Serverless") {
    private val config = ConfigFactory.load().getConfig("opendc.experiments.serverless20")

    private val routingPolicy by anyOf(RandomRoutingPolicy())

    override fun doRun(repeat: Int): Unit = runBlockingSimulation {
        val meterProvider: MeterProvider = SdkMeterProvider
            .builder()
            .setClock(clock.toOtelClock())
            .build()

        val trace = ServerlessTraceReader().parse(File(config.getString("trace-path")))
        val traceById = trace.associateBy { it.id }
        val delayInjector = FixedDelayInjector(500)
        val deployer = SimFunctionDeployer(clock, this, createMachineModel(), delayInjector) { FunctionTraceWorkload(traceById.getValue(it.name)) }
        val service =
            ServerlessService(coroutineContext, clock, meterProvider.get("opendc-serverless"), deployer, routingPolicy, FunctionTerminationPolicyFixed(coroutineContext, clock, timeout = 120 * 60 * 1000))
        val client = service.newClient()

        val sim_time = measureTimeMillis {
            coroutineScope {
                for (entry in trace) {
                    launch {
                        val function = client.newFunction(entry.id, entry.maxMemory.toLong())
                        var offset = Long.MIN_VALUE

                        for (sample in entry.samples) {
                            if (sample.invocations == 0) {
                                continue
                            }

                            if (offset < 0) {
                                offset = sample.timestamp - clock.millis()
                            }

                            delay(max(0, (sample.timestamp - offset) - clock.millis()))

                            repeat(sample.invocations) {
                                function.invoke()
                            }
                        }
                    }
                }
            }

            client.close()
            service.close()
        }

        print("Sim time = ${sim_time.toDouble() / 1000.0}\n")
    }

    private fun createMachineModel(): SimMachineModel {
        val cpuNode = ProcessingNode("Intel", "Xeon", "amd64", 2)

        return SimMachineModel(
            cpus = List(cpuNode.coreCount) { ProcessingUnit(cpuNode, it, 1000.0) },
            memory = List(18) { MemoryUnit("Crucial", "MTA18ASF4G72AZ-3G2B1", 4096.0, 4_096) }
        )
    }
}
