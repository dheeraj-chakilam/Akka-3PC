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
    upSet: Map<string, IActorRef>
    voteCount: int
    ackSet:  Set<IActorRef>
    commitIter: int
    songList: Map<string, string>
}

and Member =
    | Participant
    | Coordinator
    | Observer

and CommitState =
    | NoCommit
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
    | VoteReply of VoteMsg
    | VoteReplyTimeout
    | AckPreCommit of IActorRef
    | AckPreCommitTimeout
    | VoteReq of Update
    | PreCommit
    | PreCommitTimeout
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
            
            let newCancel =
                match state.commitState with
                | NoCommit ->
                    //TODO: Change back to NoCommit and Observer after a commit
                    scheduleRepeatedly mailbox beatrate ref (sprintf "observer %s" selfID)
                | _ ->
                    match state.coordinator with
                    | Some ref when ref = mailbox.Self ->
                        scheduleRepeatedly mailbox beatrate ref (sprintf "coordinator %s" selfID)
                    | _ ->
                        //TODO: What if state.coordinator is none?
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
            | None -> failwith "ERROR: Ref not found in beatmap in RequestFullState"

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
                        | None -> failwith "ERROR: No master in DetermineCoordinator"
                        { state with coordinator = Some mailbox.Self; commitState = CoordWaiting }
                    else
                        state
                | _ ->
                    state
            return! loop state'

        | AddSong (name, url) ->
            let state' =
                if (String.length url > int selfID + 5) then
                    { state with
                        commitState = CoordAborted }
                else
                    // Current process is the coordinator
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
        
        | VoteReply vote ->
            let state' =
                match vote with
                | Yes ->
                    // Check if we've received all votes
                    if state.voteCount + 1 = Map.count state.upSet then
                        state.upSet
                        |> Map.iter (fun _ ref -> ref <! "precommit")
                        setTimeout AckPreCommitTimeout
                        match state.commitState with
                        | CoordInitCommit decision ->
                            { state with 
                                voteCount = state.voteCount + 1
                                commitState = CoordCommitable decision }
                        | _ ->
                            failwith "ERROR: Invalid state in vote reply"
                    // If not, just increment voteCount
                    else { state with voteCount = state.voteCount + 1 }
                
                | No ->
                    state.upSet
                    |> Map.iter (fun _ ref -> ref <! "abort")
                    startCoordinatorHeartbeat {
                        state with
                            commitState = CoordAborted
                            upSet = Map.empty
                            voteCount = 0
                            commitIter = state.commitIter + 1
                            }
            return! loop state'
                        
        | VoteReplyTimeout ->
            let state' =
                match state.commitState with
                | CoordInitCommit decision ->
                    if state.voteCount = Map.count state.upSet then
                        state.upSet
                        |> Map.iter (fun _ ref -> ref <! "precommit")
                        setTimeout AckPreCommitTimeout
                        { state with commitState = CoordCommitable decision }
                    else
                        { state with commitState = CoordAborted }
                | CoordAborted ->
                    startObserverHeartbeat {
                        state with
                            commitState = CoordAborted
                            upSet = Map.empty
                            voteCount = 0
                            commitIter = state.commitIter + 1
                            }
                | _ -> failwith "ERROR: Invalid commit state in vote reply timeout"
            return! loop state'
        
        | AckPreCommit ref ->
            let state =
                let ackSet = Set.add ref state.ackSet
                if Set.count ackSet = Map.count state.upSet then
                    state.ackSet
                    |> Set.iter (fun ref -> ref <! "commit")
                    match state.commitState with
                    | CoordCommitable (Add (name, url)) ->
                        { state with
                            ackSet = ackSet
                            songList = Map.add name url state.songList
                            commitIter = state.commitIter + 1
                            commitState = CoordCommitted
                            upSet = Map.empty}
                    | CoordCommitable (Delete name) ->
                        { state with
                            ackSet = ackSet
                            songList = Map.remove name state.songList
                            commitIter = state.commitIter + 1
                            commitState = CoordCommitted
                            upSet = Map.empty}
                    | _ -> failwith "Invalid commit state in AckPreCommit"
                else
                    { state with ackSet = Set.add ref state.ackSet }
            return! loop state
        
        | AckPreCommitTimeout ->
            //TODO: Handle no acks?
            state.ackSet
            |> Set.iter (fun ref -> ref <! "commit")

        | VoteReq update ->
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
            
            // Reply to the coordinator with the vote
            match state.coordinator with
            | Some c -> c <! sprintf "votereply %s" vote
            | None -> failwith "ERROR: No coordinator in VoteReq"
            
            // Start heartbeating as a participant
            let state' = startParticipantHeartbeat {
                state with
                    commitState = ParticipantInitCommit update
                    upSet = upSet}
            
            // Wait for precommit or timeout
            setTimeout PreCommitTimeout
            
            // Start heartbeating as a participant
            return! loop state'

        | PreCommit ->
            match state.coordinator with
            | Some c -> c <! "ackprecommit"
            | None -> failwith "ERROR: No Coordinator in PreCommit"

            match state.commitState with
            | ParticipantInitCommit update ->
                let state' = startParticipantHeartbeat {
                    state with
                        commitState = ParticipantCommitable update
                        upSet = state.upSet}
            
                // Wait for precommit or timeout
                setTimeout PreCommitTimeout
            
                // Start heartbeating as a participant
                return! loop state'
            | _ ->
                failwith "ERROR: Invalid commit state in Precommit"
        
        | Decision decision ->
            let state' =
                match decision with
                | Abort -> startObserverHeartbeat {
                    state with
                        commitState = ParticipantAborted
                        upSet = Map.empty
                        commitIter = state.commitIter + 1 }
                | Commit ->
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
                    | _ -> failwith "ERROR: Invalid commit state in Decision"
             
            return! loop state'

        | GetSong name ->
            let url =
                state.songList
                |> Map.tryFind name
                |> Option.defaultValue "NONE"

            match state.master with
            | Some m -> m <! (sprintf "resp %s" url)
            | None -> failwith "ERROR: No Master in GetSong"
            
            return! loop state

        | Leave ref ->
            return! loop { state with actors = Set.remove ref state.actors }
    }

    //TODO: Check if somebody is alive and ask for state
    //TODO: Decide what the state transmission should contain
    scheduleOnce mailbox aliveThreshold mailbox.Self RequestFullState
    |> ignore

    //TODO: Check if DT Log exists if nobody is alive

    // Concurrently, try to determine whether a coordinator exists
    scheduleOnce mailbox aliveThreshold mailbox.Self DetermineCoordinator
    |> ignore
    
    // TODO: Read from DTLog

    loop {
        actors = Set.empty ;
        coordinator = None ;
        master = None ;
        beatmap = Map.empty ;
        songList = Map.empty ;
        commitState = NoCommit ;
        commitIter = 1 
        beatCancels = [];
        upSet = Map.empty;
        voteCount = 0
        ackSet = Set.empty }