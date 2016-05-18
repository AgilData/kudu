package org.kududb.spark.kudu

import java.util.concurrent.ConcurrentHashMap

import org.kududb.client.{AsyncKuduClient, KuduClient}

object ConnectionCache {
  val syncCache = new ConcurrentHashMap[String, KuduClient]()
  val asyncCache = new ConcurrentHashMap[String, KuduClient]()

  def getSyncClient(kuduMaster: String): KuduClient = {
    val kuduMaker = new java.util.function.Function[String, KuduClient]() {
      override def apply(kuduMaster: String): KuduClient = {
        new KuduClient.KuduClientBuilder(kuduMaster).build()
      }
    }
    syncCache.computeIfAbsent(kuduMaster, kuduMaker)
  }

  def getAsyncClient(kuduMaster: String): KuduClient = {
    val kuduMaker = new java.util.function.Function[String, KuduClient]() {
      override def apply(kuduMaster: String): AsyncKuduClient = {
        new AsyncKuduClient.AsyncKuduClientBuilder(kuduMaster).build()
      }
    }
    asyncCache.computeIfAbsent(kuduMaster, kuduMaker)
  }
}
