module ChatServer.World

open Akka.FSharp
open Akka.Actor

//TODO: Need 3PCState - Aborted, Unknown, Committable, Commited
//TODO: Need an iteration count
type RoomState = {
    actors: Set<IActorRef>
    coordinator: IActorRef option
    master: IActorRef option
    beatmap: Map<string,Member*IActorRef*int64>
    beatCancels: List<ICancelable>
    commitState: CommitState
    voteSet: Set<IActorRef>
    upSet: Map<string, IActorRef>
    ackSet:  Set<IActorRef>
    commitIter: int
    songList: Map<string, string>
}

and Member =
    | Participant
    | Coordinator
    | Observer

and CommitState =
    | Start
    | FirstTime
    | CoordWaiting
    | CoordInitCommit of Update
    | CoordCommitable of Update
    | CoordCommitted
    | CoordAborted
    | ParticipantInitCommit of Update
    | ParticipantCommitable of Update
    | ParticipantCommitted
    | ParticipantAborted

and DecisionMsg =
    | Abort
    | Commit

and Update =
    | Add of string * string
    | Delete of string

type RoomMsg =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | RequestFullState
    | FullStateRequest of IActorRef
    | FullState
    | DetermineCoordinator
    | Heartbeat of string * Member * IActorRef
    | AddSong of string * string
    | DeleteSong of string
    | VoteReply of VoteMsg * IActorRef
    | VoteReplyTimeout
    | AckPreCommit of IActorRef
    | AckPreCommitTimeout
    | VoteReq of Update
    | PreCommit
    | PreCommitTimeout
    | CommitTimeout
    | Decision of DecisionMsg
    | GetSong of string
    | Leave of IActorRef

and VoteMsg =
    | Yes
    | No

let sw =
    let sw = System.Diagnostics.Stopwatch()
    sw.Start()
    sw

let scheduleRepeatedly (sender:Actor<_>) rate actorRef message =
    sender.Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(
        System.TimeSpan.FromMilliseconds 0.,
        System.TimeSpan.FromMilliseconds rate,
        actorRef,
        message,
        sender.Self)

let scheduleOnce (sender:Actor<_>) after actorRef message =
    sender.Context.System.Scheduler.ScheduleTellOnceCancelable(
        System.TimeSpan.FromMilliseconds (float after),
        actorRef,
        message,
        sender.Self)

let room selfID beatrate aliveThreshold (mailbox: Actor<RoomMsg>) =
    let rec loop state = actor {

        // Cancel all previously set heartbeats and start anew
        let startHeartbeat membString state =
            state.beatCancels
            |> List.iter (fun c -> c.Cancel())
            let beatCancels =
                state.actors
                |> Set.toList
                |> List.map (fun actorRef ->
                    scheduleRepeatedly mailbox beatrate actorRef membString)
            { state with beatCancels = beatCancels }
        
        let startCoordinatorHeartbeat state =
            startHeartbeat (sprintf "coordinator %s" selfID) state
                
        let startParticipantHeartbeat state =
            printfn "Started participant heartbeat"
            startHeartbeat (sprintf "participant %s" selfID) state

        let startObserverHeartbeat state =
            startHeartbeat (sprintf "observer %s" selfID) state
        
        // Get a map (id, ref) of all the alive processes
        let getAliveMap () =
            state.beatmap
            |> Map.filter (fun _ (_,_,ms) -> sw.ElapsedMilliseconds - ms < aliveThreshold)
            |> Map.map (fun id (_,ref,_) -> ref)
        
        // Sends a message to self after the timeout threshold
        let setTimeout message =
            scheduleOnce mailbox aliveThreshold mailbox.Self message
            |> ignore

        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            printfn "In Join"
            let newCancel =
                match state.commitState with
                | Start ->
                    printfn "In Join -> Start"
                    //TODO: Change back to NoCommit and Observer after a commit
                    scheduleRepeatedly mailbox beatrate ref (sprintf "observer %s" selfID)
                | _ ->
                    printfn "In Join -> _ (not in Start)"
                    match state.coordinator with
                    | Some ref' when ref' = mailbox.Self ->
                        printfn "In Join -> _ -> Some coordinator"
                        scheduleRepeatedly mailbox beatrate ref (sprintf "coordinator %s" selfID)
                    | _ ->
                        //TODO: What if state.coordinator is none?
                        printfn "In Join -> _ -> _ (No coodinator)"
                        scheduleRepeatedly mailbox beatrate ref (sprintf "participant %s" selfID)

            let state' =
                { state with
                    actors = Set.add ref state.actors ;
                    beatCancels = newCancel :: state.beatCancels}
           
            return! loop state'

        | JoinMaster ref ->
            return! loop { state with master = Some ref }

        | Heartbeat (id, memb, ref) ->
            return! loop {
                state with
                    beatmap = state.beatmap |> Map.add id (memb, ref, sw.ElapsedMilliseconds) ;
                    coordinator = match memb with
                                  | Coordinator -> Some ref
                                  | _ -> state.coordinator }
        
        | RequestFullState ->
            // Find any alive participant/observer for their state 
            // TODO: Change behaviour? Maybe just make every alive process send state on join
            let aliveRef =
                state.beatmap
                |> Map.tryPick (fun _ (_, ref, lastMs) -> if (lastMs < aliveThreshold) then Some ref else None)
            match aliveRef with
            | Some ref -> ref <! "FullStateRequest"
            | None -> failwith "Ref not found in beatmap in RequestFullState"

        | FullStateRequest ref ->
            //TODO: send more state
            ref <! sprintf "songlist %A" (Map.toList state.songList)
        
        | FullState ->
            //TODO: Handle receiving state
            ()

        | DetermineCoordinator ->
            let state' =
                match state.coordinator with
                | None ->
                    // Check if 3PC is going on
                    let is3PC =
                        state.beatmap
                        |> Map.filter (fun _ (memb, _, lastMs) -> (memb = Participant) && (lastMs < aliveThreshold))
                        |> Map.isEmpty
                        |> not
                    //TODO: What if participants are alive but not in 3PC?
                    // If not, elect self as coordinator
                    if not is3PC then
                        printfn "%s is the coordinator" selfID
                        match state.master with
                        | Some m -> m <! sprintf "coordinator %s" selfID
                        | None -> failwith "No master in DetermineCoordinator"
                        startCoordinatorHeartbeat { state with coordinator = Some mailbox.Self; commitState = CoordWaiting }
                    else
                        state
                | _ ->
                    state
            return! loop state'

        | AddSong (name, url) ->
            printfn "In AddSong"
            // Current process is the coordinator
            let state' =
                if (String.length url > int selfID + 5) then
                    { state with
                        commitState = CoordAborted }
                else
                    // Get a snapshot of the upSet
                    let upSet = getAliveMap ()
                    let upListIds =
                        upSet
                        |> Map.toList
                        |> List.map (fun (id, _) -> id)
            
                    // Initiate 3PC with all alive participants by sending VoteReq
                    upSet
                    |> Map.iter (fun _ r -> r <! (sprintf "votereq add %s %s" name url))
            
                    // Wait for Votes or Timeout
                    setTimeout VoteReplyTimeout
                    |> ignore
                    { state with
                        commitState = CoordInitCommit (Add (name,url))
                        upSet = upSet}
            
            return! loop state'

        | DeleteSong name ->
            // Current process is the coordinator
            // Get a snapshot of the upSet
            let state' =
                let upSet = getAliveMap ()
                let upListIds =
                    upSet
                    |> Map.toList
                    |> List.map (fun (id, _) -> id)
            
                // Initiate 3PC with all alive participants by sending VoteReq
                upSet
                |> Map.iter (fun _ r -> r <! (sprintf "votereq delete %s" name))
            
                // Wait for Votes or Timeout
                setTimeout VoteReplyTimeout
                |> ignore
                { state with
                    commitState = CoordInitCommit (Delete name)
                    upSet = upSet}  
            
            return! loop state'
        
        | VoteReply (vote, ref) ->
            printfn "In VoteReply"
            let state' =
                match vote with
                | Yes ->
                    printfn "Received a yes vote"
                    let voteSet = Set.add ref state.voteSet
                    // Check if we've received all votes
                    if Set.count voteSet = Map.count state.upSet then
                        printfn "Received all votes"
                        state.upSet
                        |> Map.iter (fun _ ref -> ref <! "precommit")
                        setTimeout AckPreCommitTimeout
                        match state.commitState with
                        | CoordInitCommit decision ->
                            { state with 
                                voteSet = voteSet
                                commitState = CoordCommitable decision }
                        | _ ->
                            failwith "Invalid state in vote reply"
                    // If not, just increment voteCount
                    else
                        printfn "Didn't receive all votes"
                        { state with voteSet = voteSet }
                
                | No ->
                    printfn "Received a no vote"
                    state.upSet
                    |> Map.filter (fun _ ref -> not (ref = mailbox.Sender())) // Don't send abort to the process that voted no
                    |> Map.iter (fun _ ref -> ref <! "abort")
                    match state.master with
                    | Some m -> m <! "ack abort"
                    | None -> failwith "No master in VoteReply"
                    startObserverHeartbeat {
                        state with
                            commitState = CoordAborted
                            upSet = Map.empty
                            voteSet = Set.empty
                            commitIter = state.commitIter + 1 }
            return! loop state'
                        
        | VoteReplyTimeout ->
            printfn "In VoteReplyTimeout"
            let state' =
                match state.commitState with
                | CoordInitCommit decision ->
                    if Set.count state.voteSet = Map.count state.upSet then
                        failwith "In VoteReplyTimeout but have already received all votes. Should've handled this case in VoteReply."
                    else
                        state.upSet
                        |> Map.iter (fun _ ref -> ref <! "abort")
                        match state.master with
                        | Some m -> m <! "ack abort"
                        | None -> failwith "No master in VoteReply"
                        startObserverHeartbeat {
                            state with
                                commitState = CoordAborted
                                upSet = Map.empty
                                voteSet = Set.empty
                                commitIter = state.commitIter + 1 }
                | _ -> state
            return! loop state'
        
        | AckPreCommit ref ->
            printfn "In AckPreCommit"
            let state =
                let ackSet = Set.add ref state.ackSet
                if Set.count ackSet = Map.count state.upSet then
                    printfn "Received all Acks"
                    ackSet
                    |> Set.iter (fun ref -> ref <! "commit")
                    match state.master with
                    | Some m ->
                        printfn "Sending a commit"
                        m <! "ack commit"
                    | None -> failwith "No master in AckPreCommit"
                    match state.commitState with
                    | CoordCommitable (Add (name, url)) ->
                        { state with
                            ackSet = ackSet
                            songList = Map.add name url state.songList
                            commitIter = state.commitIter + 1
                            commitState = CoordCommitted
                            voteSet = Set.empty
                            upSet = Map.empty}
                    | CoordCommitable (Delete name) ->
                        { state with
                            ackSet = ackSet
                            songList = Map.remove name state.songList
                            commitIter = state.commitIter + 1
                            commitState = CoordCommitted
                            voteSet = Set.empty
                            upSet = Map.empty}
                    | CoordCommitted ->
                        printfn "WARNING: Some votes were ignored because they arrived after timeout threshold.\n"
                        state
                    | _ -> failwith "Invalid commit state in AckPreCommit"
                else
                    printfn "Didn't recieve all Acks yet"
                    { state with ackSet = Set.add ref state.ackSet }
            return! loop state
        
        | AckPreCommitTimeout ->
            printfn "In AckPreCommitTimeout"
            let state' =
                if Set.count state.ackSet = Map.count state.upSet then
                    failwith "In AckPreCommitTimeout but have already received all votes. Should've handled this case in AckPreCommit."
                else
                    state.ackSet
                    |> Set.iter (fun ref -> ref <! "commit")
                    match state.master with
                    | Some m -> m <! "ack commit"
                    | None -> failwith "No master in AckPreCommit"
                    match state.commitState with
                    | CoordCommitable (Add (name, url)) ->
                        { state with
                            songList = Map.add name url state.songList
                            commitIter = state.commitIter + 1
                            commitState = CoordCommitted
                            voteSet = Set.empty
                            upSet = Map.empty }
                    | CoordCommitable (Delete name) ->
                        { state with
                            songList = Map.remove name state.songList
                            commitIter = state.commitIter + 1
                            commitState = CoordCommitted
                            voteSet = Set.empty
                            upSet = Map.empty }
                    | _ -> state
            return! loop state'

        | VoteReq update ->
            printfn "In VoteReq"
            let upSet =
                getAliveMap ()
            // TODO: should votereq contain the coordinator ref?
            // Decide vote according to the rule
            let vote =
                match update with
                | Add (_, url) ->
                    if (String.length url > int selfID + 5) then "no" else "yes"
                | Delete _ ->
                    "yes"
            printfn "Voted %s" vote
            // Reply to the coordinator with the vote
            match state.coordinator with
            | Some c -> c <! sprintf "votereply %s" vote
            | None -> failwith "No coordinator in VoteReq"
            
            let state' = {
                state with
                    commitState = ParticipantInitCommit update
                    upSet = upSet}
            
            // Wait for precommit or timeout
            setTimeout PreCommitTimeout
            
            // Start heartbeating as a participant
            return! loop state'

        | PreCommit ->
            printfn "In PreCommit"
            match state.coordinator with
            | Some c -> c <! "ackprecommit"
            | None -> failwith "No Coordinator in PreCommit"

            match state.commitState with
            | ParticipantInitCommit update ->
                let state' = {
                    state with
                        commitState = ParticipantCommitable update
                        upSet = state.upSet}
            
                // Wait for precommit or timeout
                setTimeout CommitTimeout
            
                return! loop state'
            | _ ->
                failwith "Invalid commit state in Precommit"
        
        | PreCommitTimeout ->
            printfn "In PreCommitTimeout"
            // The coordinator may have died
            return! loop state

        | Decision decision ->
            printfn "In Decision"
            let state' =
                match decision with
                | Abort ->
                    printfn "In Decision -> Abort"
                    startObserverHeartbeat {
                        state with
                            commitState = ParticipantAborted
                            upSet = Map.empty
                            commitIter = state.commitIter + 1 }
                | Commit ->
                    printfn "In Decision -> Commit"
                    match state.commitState with
                    | ParticipantCommitable (Add (name, url)) ->
                        startObserverHeartbeat {
                            state with
                                songList = Map.add name url state.songList
                                commitIter = state.commitIter + 1
                                commitState = ParticipantCommitted
                                upSet = Map.empty }
                    | ParticipantCommitable (Delete name) ->
                        startObserverHeartbeat {
                            state with
                                songList = Map.remove name state.songList
                                commitIter = state.commitIter + 1
                                commitState = ParticipantCommitted
                                upSet = Map.empty }
                    | _ -> failwith (sprintf "Invalid commit state in Decision: %O" state.commitState)
             
            return! loop state'

        | GetSong name ->
            printfn "In GetSong"
            let url =
                state.songList
                |> Map.tryFind name
                |> Option.defaultValue "NONE"

            match state.master with
            | Some m -> m <! (sprintf "resp %s" url)
            | None -> failwith "No Master in GetSong"
            
            return! loop state

        | Leave ref ->
            return! loop { state with actors = Set.remove ref state.actors }
    }

    //TODO: Check if somebody is alive and ask for state
    //TODO: Decide what the state transmission should contain
    //scheduleOnce mailbox aliveThreshold mailbox.Self RequestFullState
    //|> ignore

    //TODO: Check if DT Log exists if nobody is alive

    // Concurrently, try to determine whether a coordinator exists
    // TODO: Replace 3000 with parameter
    scheduleOnce mailbox 3000L mailbox.Self DetermineCoordinator
    |> ignore
    
    // TODO: Read from DTLog

    loop {
        actors = Set.empty ;
        coordinator = None ;
        master = None ;
        beatmap = Map.empty ;
        songList = Map.empty ;
        commitState = Start ;
        commitIter = 1 ;
        beatCancels = [] ;
        upSet = Map.empty ;
        voteSet = Set.empty ;
        ackSet = Set.empty }