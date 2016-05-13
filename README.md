# MBrace.AWS

### Introduction 

AWS PaaS bindings for MBrace

### Building & Running Tests

Depending on your platform, you can build and run tests running `build.bat` or `build.cmd`. To successfully run unit tests, you need to have credentials set to your default profile in your local credentials store. Alternative, you could set the following environment variables:
```bash
export MBraceAWSTestRegion="eu-central-1" # default region is eu-central-1
export MBraceAWSTestProfile="default" # default profile name
export MBraceAWSTestCredentials="<access key>,<secret key>" # your access & secret keys for accessing DynamoDB
```

### Deploying an MBrace.AWS

There are currently 2 ways to deploy an MBrace.AWS cluster
* Standalone Deployments: Use `MBrace.AWS.StandaloneWorker` to manually deploy a cluster on-premises or a cloud VM.
* Elastic Beanstalk: Use the `MBrace.AWS.WebWorker` implementation to deploy a cluster using AWS Elastic Beanstalk.

### Build Status

* Windows [![Build status](https://ci.appveyor.com/api/projects/status/agctped28mcs1ukk?svg=true)](https://ci.appveyor.com/project/nessos/mbrace-aws)


### Maintainers

* [@theburningmonk](https://twitter.com/theburningmonk)
* [@eiriktsarpalis](https://twitter.com/eiriktsarpalis)
