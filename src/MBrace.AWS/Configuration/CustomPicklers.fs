module internal MBrace.AWS.Runtime.CustomPicklers

open System
open System.IO
open System.Net

open Amazon.Runtime.Internal.Transform

open MBrace.FsPickler

/// Used in enabling serialization of AWSSDK exceptions
let mkWebResponsePickler (r:IPicklerResolver) = 
    let int64 = r.Resolve<int64>()
    let bool = r.Resolve<bool>()
    let string = r.Resolve<string>()
    let httpStatusCode = r.Resolve<System.Net.HttpStatusCode>()

    let writer (w:WriteState) (value:IWebResponseData) =
        int64.Write w "ContentLength" value.ContentLength
        string.Write w "ContentType" value.ContentType
        httpStatusCode.Write w "StatusCode" value.StatusCode
        bool.Write w "IsSuccessStatusCode" value.IsSuccessStatusCode

    let reader (r:ReadState) =
        let contentLength = int64.Read r "ContentLength"
        let contentType = string.Read r "ContentType"
        let statusCode = httpStatusCode.Read r "StatusCode"
        let isSuccess = bool.Read r "IsSuccessStatusCode"
        { new IWebResponseData with
              member x.ContentLength: int64 = contentLength
              member x.ContentType: string = contentType
              member x.GetHeaderNames(): string [] = 
                  raise (System.NotImplementedException())
              member x.GetHeaderValue(headerName: string): string = 
                  raise (System.NotImplementedException())
              member x.IsHeaderPresent(headerName: string): bool = false
              member x.IsSuccessStatusCode: bool = isSuccess
              member x.ResponseBody: IHttpResponseBody = 
                  raise (System.NotImplementedException())
              member x.StatusCode: Net.HttpStatusCode = statusCode
        }

    let cloner _ v = v
    let visitor _ _ = ()

    Pickler.FromPrimitives(reader, writer, cloner, visitor, cacheByRef = true, useWithSubtypes = true)


let registerCustomPicklers() =
    FsPickler.RegisterPicklerFactory<IWebResponseData> mkWebResponsePickler