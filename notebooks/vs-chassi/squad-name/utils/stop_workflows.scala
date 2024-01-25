// Databricks notebook source
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import org.apache.http.entity.StringEntity

// COMMAND ----------
// MAGIC %md
// MAGIC # Classes de suporte
// COMMAND ----------

case class State(life_cycle_state: String, state_message: String)
case class JobRun(job_id: Long, run_id: Long, number_in_job: Long, 
                       state: State, start_time: Long, 
                       setup_duration: String, creator_user_name: String, run_name: String, 
                       run_page_url: String, run_type: String);

// COMMAND ----------
// MAGIC %md
// MAGIC # Métodos de suporte
// COMMAND ----------

def getRuns(): String = {
  val apiUrl = dbutils.notebook.getContext().apiUrl.get
  val accessToken = dbutils.notebook.getContext().apiToken.get
  val apiEndpoint = apiUrl + "/api/2.1/jobs/runs/list?active_only=true"
  
  val createConn = new java.net.URL(apiEndpoint).openConnection.asInstanceOf[java.net.HttpURLConnection]
  
  createConn.setRequestProperty("Authorization", "Bearer " + accessToken)
  createConn.setDoOutput(true)
  createConn.setRequestProperty("Accept", "application/json")
  createConn.setRequestMethod("GET")

  val responseStr = scala.io.Source.fromInputStream(createConn.getInputStream).mkString
  return responseStr
}

def cancelAllJobs(runs: List[JobRun]) = {
  
  val apiUrl = dbutils.notebook.getContext().apiUrl.get
  val accessToken = dbutils.notebook.getContext().apiToken.get
  val apiEndpoint = apiUrl + "/api/2.1/jobs/runs/cancel-all"
  val post = new HttpPost(apiEndpoint)
  val client = HttpClients.createDefault()
  
  post.addHeader("content-type", "application/json")
  post.addHeader("Authorization", "Bearer " + accessToken)
    
  def aux(jobId: Long, client: org.apache.http.impl.client.CloseableHttpClient): String = {
    if(jobId != 325035085375126L){
      var jsonString = """{"job_id":""" + jobId.toString +  """}"""
      post.setEntity(new StringEntity(jsonString))
      var response = client.execute(post)
      response.toString
      }
    "Skip"
  }
  println(runs.foreach(elem => aux(elem.job_id, client)))
}

// COMMAND ----------
// MAGIC %md
// MAGIC # Método principal
// COMMAND ----------

object StopWorkflows  {
  def main(args: Array[String]): Unit = {
    val runs : List[JobRun] =
    try {
      val responseStr = getRuns()
      implicit val formats = org.json4s.DefaultFormats
      (parse(responseStr) \ "runs").extract[List[JobRun]]
    } catch {
      case e:Exception => {
        println(e)
        null
      }
    }
    cancelAllJobs(runs)
  }
}

// COMMAND ----------
// MAGIC %md
// MAGIC # Execução do método principal
// COMMAND ----------
StopWorkflows.main(Array("Test"))
