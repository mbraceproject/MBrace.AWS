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

### Build Status

* Windows [![Build status](https://ci.appveyor.com/api/projects/status/agctped28mcs1ukk?svg=true)](https://ci.appveyor.com/project/nessos/mbrace-aws)


### Maintainers

* [@theburningmonk](https://twitter.com/theburningmonk)
* [@eiriktsarpalis](https://twitter.com/eiriktsarpalis)
