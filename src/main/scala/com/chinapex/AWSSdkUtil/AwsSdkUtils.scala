package com.chinapex.AWSSdkUtil

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.chinapex.tools.utils.TryWith
import org.junit.Test
import com.amazonaws.services.ec2.{AmazonEC2, AmazonEC2Client, AmazonEC2ClientBuilder, model}
import com.chinapex.helpers.ConfigurationHelper
import com.chinapex.tools.mapper.JsonMapper
import org.slf4j.LoggerFactory

/**
  * Created by russ on 17-7-4.
  */

case class CaseRunInstanceRequset(){
  val runInstanceRequset = new model.RunInstancesRequest()
}
object AwsSdkUtils extends TryWith{
  private var awsClient:Map[String,AwsSdkUtils]=Map()

  def getAmazonEc2(instanceId:String,accessId:String=ConfigurationHelper.awsAccessId,
                                    accessKey:String=ConfigurationHelper.awsAccessKey,
                                    regions: Regions=ConfigurationHelper.awsRegion):AwsSdkUtils={
    if(!awsClient.exists(x=>x._1.equals(accessId)))
      awsClient=awsClient.++(Map(accessId->getBuilder(instanceId:String,accessId,accessKey,regions)))
    awsClient(accessId)
  }

  @deprecated
  def getAmazonEc2WithDefault(instanceId:String):AwsSdkUtils={
    if(!awsClient.exists(x=>x._1.equals(ConfigurationHelper.awsAccessId)))
      awsClient=awsClient.++(
        Map(ConfigurationHelper.awsAccessId->
          getBuilder(
            instanceId,
            ConfigurationHelper.awsAccessId,
            ConfigurationHelper.awsAccessKey,
            ConfigurationHelper.awsRegion)
        )
      )
    awsClient(ConfigurationHelper.awsAccessId)
  }

  def getBuilder(instanceId:String,accessId:String,accessKey:String,regions: Regions):AwsSdkUtils=
    new AwsSdkUtils(
      AmazonEC2ClientBuilder.standard()
      .withRegion(Regions.CN_NORTH_1)
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessId, accessKey)))
      .build()
      ,instanceId
    )
}

class AwsSdkUtils(amazonEC2: AmazonEC2,instanceId:String) extends TryWith{
  private  val logger = LoggerFactory.getLogger(this.getClass)


  def rebootInstance:model.RebootInstancesResult={
    tryWith(amazonEC2.rebootInstances(new model.RebootInstancesRequest().withInstanceIds(instanceId)))(
      inException = e=>{
        logger.error(s"AWSCtrlActor restart get exception:${e.getMessage}${e.printStackTrace()}")
        new model.RebootInstancesResult()
      }
    )
  }

  def startInstance:model.StartInstancesResult= {
    tryWith(amazonEC2.startInstances(new model.StartInstancesRequest().withInstanceIds(instanceId)))(
      inException = e=>{
        logger.error(s"AWSCtrlActor restart get exception:${e.getMessage}${e.printStackTrace()}")
        new model.StartInstancesResult()
      }
    )
  }


  def stopInstance:model.StopInstancesResult= {
    tryWith(amazonEC2.stopInstances(new model.StopInstancesRequest().withInstanceIds(instanceId)))(
      inException = e=>{
        logger.error(s"AWSCtrlActor restart get exception:${e.getMessage}${e.printStackTrace()}")
        new model.StopInstancesResult()
      }
    )
  }

  def testGetDescribe() = {
     tryWith(
       amazonEC2.describeInstanceStatus().toString
     )(
      inException = e=>{
        logger.error(s"AWSCtrlActor testGetDescribe  get exception:${e.getMessage}${e.printStackTrace()}")
        "failed"
      }
    )
  }


}


class TestAwsSdkUtils extends TryWith{

  @Test
  def testSdk(): Unit ={
    println("================test Start=======================")

    println(s"is Region available: ${Region.getRegion(Regions.CN_NORTH_1)
      .isServiceSupported(AmazonDynamoDB.ENDPOINT_PREFIX)}")

    val ec2Builder =AwsSdkUtils.getAmazonEc2("i-0f3857fab07cbc6d4")


    println("test builder successful")
    println("test describeAvailabilityZones")
    println("==================================")
    println(ec2Builder.testGetDescribe())
    println("==================================")
    println("test successful")

  }

}
