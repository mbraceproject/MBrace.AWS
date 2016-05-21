#I __SOURCE_DIRECTORY__
#r "tools/Newtonsoft.Json.dll"
#r "tools/FsPickler.Json.dll"
#r "tools/FsPickler.dll"
#r "tools/Mono.Cecil.dll"
#r "tools/Vagabond.AssemblyParser.dll"
#r "tools/Vagabond.dll"
#r "tools/Argu.dll"
#r "tools/Unquote.dll"
#r "tools/FSharp.AWS.DynamoDB.dll"
#r "tools/AWSSDK.Core.dll"
#r "tools/AWSSDK.DynamoDBv2.dll"
#r "tools/AWSSDK.S3.dll"
#r "tools/AWSSDK.SQS.dll"
#r "tools/MBrace.Core.dll"
#r "tools/MBrace.Runtime.dll"
#r "tools/MBrace.AWS.dll"

open System.IO
open MBrace.AWS

AWSWorker.LocalExecutable <- Path.Combine(__SOURCE_DIRECTORY__, "tools/mbrace.awsworker.exe")