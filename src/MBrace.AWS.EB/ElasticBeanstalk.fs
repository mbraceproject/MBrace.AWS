module MBrace.AWS.ElasticBeanstalk

open Amazon.ElasticBeanstalk
open Amazon.ElasticBeanstalk.Model
open Amazon.Runtime

let region = AWSRegion.EUCentral1
let credentials = MBraceAWSCredentials.FromCredentialsStore()

let client = new AmazonElasticBeanstalkClient(credentials, Amazon.RegionEndpoint.EUCentral1)


let car = new CreateApplicationRequest("mbrace-docker-2")

let app = client.CreateApplication(car)

let ct = new CreateConfigurationTemplateRequest("mbrace-docker-2", "mbrace-docker-2")

ct.EnvironmentId <- "mbrace-docker-2"

client.CreateConfigurationTemplate(ct)

//let env = new CreateEnvironmentRequest("mbrace-docker-2", "mbrace-docker-2-env")
//let resp = client.CreateEnvironment(env)
//
//env.OptionSettings.Add (new Confi)