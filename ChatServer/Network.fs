module ChatServer.Network

open System.Text
open System.Net
open Akka.FSharp
open Akka.IO
open World
open System.Diagnostics

type Server =
    | ChatServer
    | MasterServer

let handler world serverType selfID connection (mailbox: Actor<obj>) =  
    let rec loop connection = actor {
        let! msg = mailbox.Receive()

        match msg with
        | :? Tcp.Received as received ->
            //In case we receive multiple messages (delimited by a newline) in the same Tcp.Received message
            let lines = (Encoding.ASCII.GetString (received.Data.ToArray())).Trim().Split([|'\n'|])
            Array.iter (fun (line:string) ->
                let data = line.Split([|' '|], 2)

                match data with
                | [| "participant"; message |] ->
                    world <! Heartbeat (message.Trim(), Participant, mailbox.Self)

                | [| "coordinator"; message |] ->
                    world <! Heartbeat (message.Trim(), Coordinator, mailbox.Self)
                
                | [| "observer"; message |] ->
                    world <! Heartbeat (message.Trim(), Observer, mailbox.Self)

                | [| "add"; message |] ->
                    match message.Trim().Split([|' '|], 2) with
                    | [| name; url |] -> world <! AddSong (name, url)
                    | _ -> failwith "Invalid add song request"
                
                | [| "get"; message |] ->
                    world <! GetSong (message.Trim())
                
                | [| "delete"; message |] ->
                    world <! DeleteSong (message.Trim())

                | [| "votereq"; message |] ->
                    match message.Trim().Split([|' '|]) with
                    | [| "add"; name; url |] -> world <! VoteReq (Add (name, url))
                    | [| "delete"; name |] -> world <! VoteReq (Delete name)
                    | _ -> failwith "Invalid votereq"

                | [| "votereply"; message |] -> 
                    match message.Trim() with
                    | m when m = "yes" -> world <! VoteReply (Yes, mailbox.Self)
                    | m when m = "no" -> world <! VoteReply (No, mailbox.Self)
                    | _ ->
                        failwith "Invalid votereply"
                
                | [| "precommit" |] ->
                    world <! PreCommit
                
                | [| "ackprecommit" |] -> 
                    world <! AckPreCommit mailbox.Self
                
                | [| "commit" |] ->
                    world <! Decision Commit

                | [| "abort" |] ->
                    world <! Decision Abort 
                
                | [| "crash" |] ->
                    System.Environment.Exit(0)
                
                | [| "crashAfterVote" |] ->
                    world <! SetCrashAfterVote

                | [| "crashBeforeVote" |] ->
                    world <! SetCrashBeforeVote

                | [| "crashAfterAck" |] ->
                    world <! SetCrashAfterAck
                
                | [| "crashVoteREQ" |] ->
                    world <! SetCrashVoteReq Set.empty

                | [| "crashVoteREQ"; message |] ->
                    let idSet =
                        message.Trim().Split([|' '|])
                        |> Set.ofArray
                    world <! SetCrashVoteReq idSet

                | [| "crashPartialPreCommit"|] -> 
                    world <! SetCrashPartialPreCommit Set.empty
                
                | [| "crashPartialPreCommit"; message |] -> 
                    let idSet =
                        message.Trim().Split([|' '|])
                        |> Set.ofArray
                    world <! SetCrashPartialPreCommit idSet

                | [| "crashPartialCommit" |] ->
                    world <! SetCrashPartialCommit Set.empty
                
                | [| "crashPartialCommit"; message |] ->
                    let idSet =
                        message.Trim().Split([|' '|])
                        |> Set.ofArray
                    world <! SetCrashPartialCommit idSet

                | [| "statereq" |] ->
                    world <! StateReq mailbox.Self
                
                | [| "statereqreply"; message |] ->
                    match message.Trim() with
                    | m when m = "aborted" -> world <! StateReqReply (mailbox.Self, Aborted)
                    | m when m = "uncertain" -> world <! StateReqReply (mailbox.Self, Uncertain)
                    | m when m = "committable" -> world <! StateReqReply (mailbox.Self, Committable)
                    | m when m = "committed" -> world <! StateReqReply (mailbox.Self, Committed)
                    | _ ->
                        failwith "Invalid statereqreply"
        
                | _ ->
                    connection <! Tcp.Write.Create (ByteString.FromString <| sprintf "Invalid request. (%A)\n" data)) lines
    
        | :? Tcp.ConnectionClosed as closed ->
            world <! Leave mailbox.Self
            mailbox.Context.Stop mailbox.Self

        | :? string as response ->
            connection <! Tcp.Write.Create (ByteString.FromString (response + "\n"))

        | _ -> mailbox.Unhandled()

        return! loop connection
    }

    match serverType with
    | ChatServer -> world <! Join mailbox.Self
    | MasterServer -> world <! JoinMaster mailbox.Self
    
    loop connection

let server world serverType port selfID max (mailbox: Actor<obj>) =
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()
        
        match msg with
        | :? Tcp.Bound as bound ->
            printf "Listening on %O\n" bound.LocalAddress

        | :? Tcp.Connected as connected -> 
            printf "%O connected as %O\n" connected.RemoteAddress serverType
            let handlerName = "handler_" + connected.RemoteAddress.ToString().Replace("[", "").Replace("]", "")
            let handlerRef = spawn mailbox handlerName (handler world serverType selfID sender)
            sender <! Tcp.Register handlerRef

        | _ -> mailbox.Unhandled()

        return! loop()
    }

    // Start listening on port for connections
    mailbox.Context.System.Tcp() <! Tcp.Bind(mailbox.Self, IPEndPoint(IPAddress.Any, port),options=[Inet.SO.ReuseAddress(true)])

    // If a chatserver, try to connect to all the other ports (only once on startup)
    if serverType = ChatServer then
        let clientPortList = seq {0 .. max} |> Seq.filter (fun n -> n <> int selfID) |> Seq.map (fun n -> 20000 + n)
        for p in clientPortList do
            mailbox.Context.System.Tcp() <! Tcp.Connect(IPEndPoint(IPAddress.Loopback, p),options=[Inet.SO.ReuseAddress(true)])

    loop()