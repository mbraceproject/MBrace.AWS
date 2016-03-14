#I "../../bin"
#r "FsPickler.dll"
#r "FsPickler.Json.dll"
#r "AWSSDK.Core.dll"
#r "AWSSDK.S3.dll"
#r "AWSSDK.DynamoDBv2.dll"
#r "AWSSDK.SQS.dll"
#r "Vagabond.dll"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "MBrace.AWS.dll"


open Amazon
open Amazon.Runtime
open Amazon.S3
open Amazon.SQS
open Amazon.DynamoDBv2

open MBrace.Core
open MBrace.Core.Internals
open MBrace.AWS
open MBrace.AWS.Runtime
open MBrace.AWS.Store

AWSWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.awsworker.exe"
let config = Configuration.FromCredentialsStore(AWSRegion.EUCentral1, resourcePrefix = "tests4820")

let cluster = AWSCluster.InitOnCurrentMachine(config, workerCount = 1, logger = ConsoleLogger())
//let cluster = AWSCluster.Connect(config, logger = ConsoleLogger())
cluster.Reset(reactivate = false, force = true)

cluster.Run(cloud { return 42 })

cluster.CullNonResponsiveWorkers(System.TimeSpan.FromSeconds 5.)

let proc = cluster.CreateProcess(cloud { return! Cloud.Parallel [for i in 1 .. 1000 -> cloud { return i }]})

proc.Cancel()

proc.Status
cluster.Workers

cluster.ShowWorkers()
cluster.ClearSystemLogs()

cluster.ShowSystemLogs()

cluster.ShowProcesses()
cluster.ClearAllProcesses()

let worker = cluster.Workers.[0] :> IWorkerRef
let proc' = cluster.CreateProcess(Cloud.Sleep 10000, target = worker)
proc'.Result

cloud { let! w = Cloud.CurrentWorker in return Some w }
|> Cloud.ChoiceEverywhere
|> cluster.Run
//let workers = cluster.Run(Cloud.ChoiceEverywhere { let! w = Cloud.CurrentWorker )

let c = cluster.Run(test())

c.Value

CloudAtom.Increment c |> cluster.RunLocally


let test () = cloud {
    let! counter = CloudAtom.New 0
    let worker i j = cloud {
        if i = 0 && j = 0 then
            invalidOp "failure"
        else
            do! Cloud.Sleep 30000
            do! CloudAtom.Increment counter |> Local.Ignore
    }

    let cluster i = Array.init 10 (worker i) |> Cloud.Parallel |> Cloud.Ignore
    try do! Array.init 10 cluster |> Cloud.Parallel |> Cloud.Ignore
    with :? System.InvalidOperationException -> ()
    return counter
} 


let cv = cluster.Run(test())

cv.Value

//|> runOnCloud |> Choice.shouldEqual 0)


let test'() = cloud {
    let! counter = CloudAtom.New 0
    let worker i = cloud { 
        if i = 15 then
            invalidOp "failure"
        else
            do! Cloud.Sleep 5000
            do! CloudAtom.Increment counter |> Local.Ignore
    }

    try do! Array.init 32 worker |> Cloud.Parallel |> Cloud.Ignore
    with :? System.InvalidOperationException -> return ()
    return counter
}

let cv' = cluster.Run(test'())

cv'.Value

cluster.ShowProcesses()

let proc = cluster.GetProcessById "69c85695-b8fe-4472-ad26-9c5e1863d466"
proc.Cancel()

[<AbstractClass>]
type GenericList() =
    abstract Tail : GenericList option
    abstract Length : int
    abstract Accept : IListVisitor<'R> -> 'R

and Empty() =
    inherit GenericList()
    override __.Tail = None
    override __.Length = 0
    override __.Accept v = v.Empty()

and IConsVisitor<'R> =
    abstract Cons<'T> : head:'T * tail:GenericList -> 'R

and ICons =
    abstract Accept: IConsVisitor<'R> -> 'R

and Cons<'T>(value : 'T, tail : GenericList) =
    inherit GenericList()

    member __.Head = value
    override __.Tail = None
    override __.Length = 1 + tail.Length
    override __.Accept v = v.Cons(value, tail)
    interface ICons with
        member __.Accept v = v.Cons(value, tail)

and IListVisitor<'R> =
    inherit IConsVisitor<'R>
    abstract Empty : unit -> 'R


let empty = new Empty()

let (@@) (head : 'T) (tail : GenericList) = new Cons<'T>(head, tail) :> GenericList

let ml = 1 @@ "1" @@ empty


type Iterator =
    abstract Iter<'T> : 'T -> unit

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module GenericList =
    let iter (f : Iterator) (list : GenericList) =
        let rec aux (l : GenericList) =
            l.Accept {
                new IListVisitor<bool> with
                    member __.Empty() = false
                    member __.Cons(h,t) = f.Iter h ; aux t
            }

        aux list |> ignore


ml |> GenericList.iter { new Iterator with 
                            member __.Iter<'T> (t : 'T) = printfn "%O" t }