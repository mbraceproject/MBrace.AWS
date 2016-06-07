module MBrace.AWS.ElasticBeanstalk

open Amazon.ElasticBeanstalk
open Amazon.ElasticBeanstalk.Model
open Amazon.Runtime

let region = AWSRegion.EUCentral1
let credentials = MBraceAWSCredentials.FromCredentialsStore()

let client = new AmazonElasticBeanstalkClient(credentials, Amazon.RegionEndpoint.EUCentral1)

let appName = "mbrace-docker"
let envName = "mbrace-docker"

let car = new CreateApplicationRequest(appName)
car.Description <- appName

let app = client.CreateApplication(car)
app

////--------------------------------------------------

let env = new CreateEnvironmentRequest(appName, envName)
//let res = client.ListAvailableSolutionStacks()
//res.SolutionStacks |> Seq.filter (fun s -> s.ToLower().Contains "running docker") |> Seq.toArray
env.SolutionStackName <- "64bit Amazon Linux 2016.03 v2.1.0 running Docker 1.9.1"
env.Tier <- new EnvironmentTier(Name = "Worker", Type = "SQS/HTTP")
env.OptionSettings.Add <| new ConfigurationOptionSetting("aws:autoscaling:asg", "MinSize", "4")
env.OptionSettings.Add <| new ConfigurationOptionSetting("aws:autoscaling:asg", "MaxSize", "4")
env.OptionSettings.Add <| new ConfigurationOptionSetting("aws:elasticbeanstalk:application:environment", "AWS_ACCESS_KEY_ID", "my_aws_key_text")
env.OptionSettings.Add <| new ConfigurationOptionSetting("aws:elasticbeanstalk:application:environment", "AWS_SECRET_ACCESS_KEY", "my_aws_key_text")

let resp = client.CreateEnvironment(env)

resp.Resources

client.DeleteApplication(new DeleteApplicationRequest(appName))