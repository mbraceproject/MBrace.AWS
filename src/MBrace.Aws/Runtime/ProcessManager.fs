namespace MBrace.Aws.Runtime

open System
open System.Runtime.Serialization

open Nessos.FsPickler

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Components
open MBrace.Aws
open MBrace.Aws.Runtime.Utilities

[<Sealed; AutoSerializable(false)>]
type CloudProcessManager private (clusterId : ClusterId, logger : ISystemLogger) =

    interface ICloudProcessManager with
        member __.ClearProcess(taskId: string) = async {
            let record = new CloudProcessRecord(taskId)
            record.ETag <- "*"
            do! Table.delete 
                    clusterId.DynamoDBAccount 
                    clusterId.RuntimeTable 
                    record // TODO : perform full cleanup?
        }
        
        member __.ClearAllProcesses() = async {
            let! records = 
                Table.query<CloudProcessRecord> 
                    clusterId.DynamoDBAccount 
                    clusterId.RuntimeTable 
                    CloudProcessRecord.DefaultHashKey
            do! Table.deleteBatch 
                    clusterId.DynamoDBAccount 
                    clusterId.RuntimeTable 
                    records
        }
        
        member __.StartProcess(info: CloudProcessInfo) = async {
            let taskId = guid()
            logger.Logf LogLevel.Info "Creating cloud process %A" taskId
            let record = CloudProcessRecord.CreateNew(taskId, info)
            do! Table.put clusterId.DynamoDBAccount clusterId.RuntimeTable record
            let tcs = new CloudProcessEntry(clusterId, taskId, info)
            return tcs :> ICloudProcessEntry
        }
        
        member __.GetAllProcesses() = async {
            let! records = 
                Table.query<CloudProcessRecord> 
                    clusterId.DynamoDBAccount 
                    clusterId.RuntimeTable 
                    CloudProcessRecord.DefaultHashKey
            return records 
                   |> Seq.map(fun r ->
                        new CloudProcessEntry(
                            clusterId, r.Id, r.ToCloudProcessInfo()) 
                        :> ICloudProcessEntry) 
                   |> Seq.toArray
        }
        
        member __.TryGetProcessById(taskId: string) = async {
            let! record = 
                Table.read<CloudProcessRecord> 
                    clusterId.DynamoDBAccount
                    clusterId.RuntimeTable
                    CloudProcessRecord.DefaultHashKey
                    taskId
            match record with
            | null -> return None 
            | _ -> 
                let entry = 
                    new CloudProcessEntry(
                        clusterId, taskId, record.ToCloudProcessInfo())
                    :> ICloudProcessEntry
                return Some entry
        }

    static member Create(clusterId : ClusterId, logger) = 
        new CloudProcessManager(clusterId, logger)