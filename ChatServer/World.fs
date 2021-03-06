﻿module ChatServer.World

open Akka.FSharp
open Akka.Actor

type RoomState = {
    actors: Set<IActorRef>
    coordinator: IActorRef option
    master: IActorRef option
    beatmap: Map<string,Member*IActorRef*int64>
    beatCancels: List<ICancelable>
    commitPhase: CommitPhase
    commitIter: int
    songList: Map<string, string>
    crash: CrashType option
}

and Member =
    | Participant
    | Coordinator
    | Observer

and CommitPhase =
    | Start
    | CoordWaiting
    | CoordInitCommit of Update * Map<string, IActorRef> * Set<IActorRef> // The voteSet
    | CoordCommittable of Update * Map<string, IActorRef> * Set<IActorRef> // The ackSet
    | CoordCommitted
    | CoordAborted
    | ParticipantInitCommit of Update * Map<string, IActorRef>
    | ParticipantCommittable of Update * Map<string, IActorRef>
    | ParticipantCommitted
    | ParticipantAborted

and DecisionMsg =
    | Abort
    | Commit

and Update =
    | Add of string * string
    | Delete of string

and CrashType =
    | CrashAfterVote
    | CrashBeforeVote
    | CrashAfterAck
    | CrashVoteReq of Set<string>
    | CrashPartialPreCommit of Set<string>
    | CrashPartialCommit of Set<string>

type RoomMsg =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | FullState of int * Map<string, string>
    | DetermineCoordinator
    | Heartbeat of string * Member * IActorRef
    | AddSong of string * string
    | DeleteSong of string
    | VoteReply of VoteMsg * IActorRef
    | VoteReplyTimeout of int
    | AckPreCommit of IActorRef
    | AckPreCommitTimeout of int
    | StateReqReply of IActorRef * CommitState
    | StateReqReplyTimeout of int
    | VoteReq of Update
    | PreCommit
    | PreCommitTimeout of int
    | CommitTimeout of int
    | Decision of DecisionMsg
    | StateReq of IActorRef
    | StateReqTimeout of int
    | ObserverCheckCoordinator of int
    | GetSong of string
    | Leave of IActorRef
    | SetCrashAfterVote
    | SetCrashBeforeVote
    | SetCrashAfterAck
    | SetCrashVoteReq of Set<string>
    | SetCrashPartialPreCommit of Set<string>
    | SetCrashPartialCommit of Set<string>

and VoteMsg =
    | Yes
    | No

and CommitState =
    | Aborted
    | Uncertain
    | Committable
    | Committed

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

let delayExit time =
    async {
        do! Async.Sleep (int time)
        exit(0) }
    |> Async.StartImmediate

let room selfID beatrate aliveThreshold timeout delay (mailbox: Actor<RoomMsg>) =
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
            printfn "Started coordinator heartbeat"
            startHeartbeat (sprintf "coordinator %s" selfID) state
                
        let startParticipantHeartbeat state =
            printfn "Started participant heartbeat"
            startHeartbeat (sprintf "participant %s" selfID) state

        let startObserverHeartbeat state =
            startHeartbeat (sprintf "observer %s" selfID) state
        
        let filterAlive map =
            map
            |> Map.filter (fun _ (_,_,ms) -> (sw.ElapsedMilliseconds - ms) < aliveThreshold)

        // Get a map (id, ref) of all the alive processes
        let getAliveMap state =
            state.beatmap
            |> filterAlive
            |> Map.map (fun id (_,ref,_) -> ref)
        
        let getAlive membType state =
            state.beatmap
            |> filterAlive
            |> Map.filter (fun id (memb, _, _) -> memb = membType)
            |> Map.map (fun id (_,ref,_) -> ref)

        // Sends a message to self after the timeout threshold
        let setTimeout (message: RoomMsg) =
            scheduleOnce mailbox timeout mailbox.Self message
            |> ignore

        let initiateElectionProtocol state =
            // Find a new coordinator
            let aliveParticipants =
                state
                |> getAlive Participant
            let (potentialCoordId,potentialCoordRef) =
                aliveParticipants
                |> Map.fold (fun (lowest, _) id ref -> if (int id) < (int lowest) then (id, ref) else (lowest, ref)) (selfID, mailbox.Self)
            if potentialCoordId <> selfID then
                printfn "Not the new coordinator"
                // This process is not the new potential coordinator
                setTimeout <| StateReqTimeout state.commitIter
                { state with coordinator = Some potentialCoordRef }
            else
                printfn "Elected as the new coordinator"
                // This process is the new coordinator
                // Convert commitPhase from participant to coordinator
                let state' =
                    match state.commitPhase with
                    | ParticipantInitCommit (update, _) ->
                        { state with
                            commitPhase = CoordInitCommit (update, aliveParticipants, Set.empty) }
                    | ParticipantCommittable (update, _) ->
                        { state with
                            commitPhase = CoordCommittable (update, aliveParticipants, Set.empty) }
                    | ParticipantCommitted ->
                        { state with
                            commitPhase = CoordCommitted }
                    | ParticipantAborted ->
                        { state with
                            commitPhase = CoordAborted }
                    | _ ->
                        printfn "ERROR: Invalid commit phase in initiateElectionProtocol"
                        state
                // Start heartbeating as the coordinator
                let state'' =
                    startCoordinatorHeartbeat { state' with coordinator = Some mailbox.Self }
                aliveParticipants
                |> Map.iter (fun _ ref -> ref <! "statereq")
                setTimeout <| StateReqReplyTimeout state.commitIter
                state''

        let initiateObserverElectionProtocol state =
            printfn "In initiateObserverElectionProtocol"
            let aliveMap = getAliveMap state
            // Find a new coordinator
            let (potentialCoordId,potentialCoordRef) =
                aliveMap
                |> Map.fold (fun (lowest, _) id ref -> if (int id) < (int lowest) then (id, ref) else (lowest, ref)) (selfID, mailbox.Self)
            if potentialCoordId <> selfID then
                // This process is not the new potential coordinator
                setTimeout <| ObserverCheckCoordinator state.commitIter
                { state with coordinator = Some potentialCoordRef }
            else
                // Start heartbeating as the coordinator
                match state.master with
                | Some m -> m <! sprintf "coordinator %s" selfID
                | None -> printfn "WARNING: No master in DetermineCoordinator"
                startCoordinatorHeartbeat { state with coordinator = Some mailbox.Self }

        let sendFullState state ref =
            printfn "Sending full state"
            let songsString =
                state.songList
                |> Map.toSeq
                |> Seq.map (fun (name,url) -> sprintf "%s,%s" name url)
                |> String.concat "|"
            ref <! sprintf "fullstate iter %i songs %s" state.commitIter songsString
        
        let sendFullStatetoObservers state =
            state
            |> getAlive Observer
            |> Map.iter (fun _ ref -> sendFullState state ref)
            state
        
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            printfn "In Join"
            sendFullState state ref
            let newCancel =
                match state.commitPhase with
                | Start ->
                    scheduleRepeatedly mailbox beatrate ref (sprintf "observer %s" selfID)
                | _ ->
                    match state.coordinator with
                    | Some ref' when ref' = mailbox.Self ->
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
        
        | FullState (sourceiter, songList) ->
            printfn "Received a fullstate message of sourceiter %i" sourceiter
            let state' = 
                if (sourceiter > state.commitIter) then
                    {state with songList = songList; commitIter = sourceiter}
                else 
                    state
            return! loop state'

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
                    let isNobodyAlive =
                        Map.isEmpty state.beatmap
                   
                    match is3PC, isNobodyAlive with
                    | false, true ->
                        printfn "%s is the coordinator" selfID
                        match state.master with
                        | Some m -> m <! sprintf "coordinator %s" selfID
                        | None -> printfn "WARNING: No master in DetermineCoordinator"
                        startCoordinatorHeartbeat { state with coordinator = Some mailbox.Self; commitPhase = CoordWaiting }
                    | false, false ->
                        setTimeout <| ObserverCheckCoordinator state.commitIter
                        state
                    | _ ->
                        state

                | _ ->
                    state
            return! loop state'

        | AddSong (name, url) ->
            printfn "In AddSong"
            // Current process is the coordinator
            let state' =
                if (String.length url > int selfID + 5) then
                    printfn "Aborted by self (coordinator) because the url doesn't satisfy the length condition"
                    match state.master with
                    | Some m -> m <! "ack abort"
                    | None -> printfn "ERROR: No master in AddSong"
                    { state with
                        commitPhase = CoordAborted
                        commitIter = state.commitIter }
                else
                    // Get a snapshot of the upSet
                    let upSet = getAliveMap state
            
                    match state.crash with
                    | Some (CrashVoteReq crashSet) ->
                        upSet
                        |> Map.filter (fun id _ -> Set.contains id crashSet)
                        |> Map.iter (fun _ r -> r <! (sprintf "votereq add %s %s" name url))
                        delayExit delay
                    | _ ->
                        // Initiate 3PC with all alive participants by sending VoteReq
                        upSet
                        |> Map.iter (fun _ r -> r <! (sprintf "votereq add %s %s" name url))
                    
                    // Wait for Votes or Timeout
                    setTimeout <| VoteReplyTimeout state.commitIter
                    |> ignore
                    { state with
                        commitPhase = CoordInitCommit (Add (name,url), upSet, Set.empty) }
            
            return! loop state'

        | DeleteSong name ->
            // Current process is the coordinator
            // Get a snapshot of the upSet
            let state' =
                let upSet = getAliveMap state
                let upListIds =
                    upSet
                    |> Map.toList
                    |> List.map (fun (id, _) -> id)
            
                // Initiate 3PC with all alive participants by sending VoteReq
                upSet
                |> Map.iter (fun _ r -> r <! (sprintf "votereq delete %s" name))
            
                // Wait for Votes or Timeout
                setTimeout <| VoteReplyTimeout state.commitIter
                |> ignore
                { state with
                    commitPhase = CoordInitCommit ((Delete name), upSet, Set.empty) }
            
            return! loop state'
        
        | VoteReply (vote, ref) ->
            printfn "In VoteReply"
            let state' =
                match state.commitPhase with
                | CoordInitCommit (update, upSet, voteSet) ->
                    match vote with
                    | Yes ->
                        printfn "Received a yes vote"
                        let voteSet' = Set.add ref voteSet
                        // Check if we've received all votes
                        if Set.count voteSet' = Map.count upSet then
                            //printfn "Received all votes"
                            match state.crash with
                            | Some (CrashPartialPreCommit crashSet) ->
                                //printfn "About to send partial precommits.\n The crashSet is %O\n The upSet is %O" crashSet upSet
                                upSet
                                |> Map.filter (fun id _ -> Set.contains id crashSet) 
                                |> Map.iter (fun _ ref -> ref <! "precommit")
                                setTimeout <| AckPreCommitTimeout state.commitIter
                                delayExit delay
                            | _ ->
                                upSet
                                |> Map.iter (fun _ ref -> ref <! "precommit")
                                setTimeout <| AckPreCommitTimeout state.commitIter
                            // Move on to the next commit phase
                            { state with 
                                commitPhase = CoordCommittable (update, upSet, Set.empty)}
                        else
                            printfn "Didn't receive all votes"
                            // If not, just add new vote to voteset'
                            { state with 
                                commitPhase = CoordInitCommit (update, upSet, voteSet')}
                    | No ->
                        printfn "Received a no vote"
                        upSet
                        |> Map.filter (fun _ ref -> not (ref = mailbox.Sender())) // Don't send abort to the process that voted no
                        |> Map.iter (fun _ ref -> ref <! "abort")
                        match state.master with
                        | Some m -> m <! "ack abort"
                        | None -> printfn "WARNING: No master in VoteReply"
                        sendFullStatetoObservers {
                            state with
                                commitPhase = CoordAborted
                                commitIter = state.commitIter + 1 }
                | _ ->
                    printfn "WARNING: Invalid state in vote reply"
                    state
            return! loop state'
                        
        | VoteReplyTimeout sourceIter ->
            printfn "In VoteReplyTimeout"
            let state' =
                match state.commitPhase with
                | CoordInitCommit (update, upSet, voteSet) ->
                    if Set.count voteSet = Map.count upSet then
                        printfn "Since there is no participant, short circuiting to committing by myself"
                        if (Map.count upSet) = 0 then
                            // If the coordinator is the only server alive, just commit the decision
                                // Send a commit to master
                                match state.master with
                                | Some m ->
                                    printfn "Sending a commit to master"
                                    m <! "ack commit"
                                | None ->
                                    printfn "WARNING: No master"
                                match update with
                                | (Add (name, url)) when sourceIter = state.commitIter ->
                                    sendFullStatetoObservers {
                                        state with
                                            songList = Map.add name url state.songList
                                            commitIter = state.commitIter + 1
                                            commitPhase = CoordCommitted }
                                | (Delete name) when sourceIter = state.commitIter ->
                                    sendFullStatetoObservers {
                                        state with
                                            songList = Map.remove name state.songList
                                            commitIter = state.commitIter + 1
                                            commitPhase = CoordCommitted }
                                | _ ->
                                    printfn "Received a VoteReplyTimeout in a later iteration"
                                    state
                        else
                            printfn "In VoteReplyTimeout but have already received all votes."
                            state
                    else
                        // We did not receive all vote replies
                        printfn "Didn't receive all vote replies. Aborting."
                        upSet
                        |> Map.iter (fun _ ref -> ref <! "abort")
                        match state.master with
                        | Some m -> m <! "ack abort"
                        | None -> printfn "WARNING: No master in VoteReply"
                        sendFullStatetoObservers {
                            state with
                                commitPhase = CoordAborted
                                commitIter = state.commitIter + 1 }
                | s ->
                    printfn "WARNING: Invalid state in VoteReplyTimeout: %O" s
                    state
            return! loop state'
        
        | AckPreCommit ref ->
            printfn "In AckPreCommit"
            let state =
                match state.commitPhase with
                | CoordCommittable (decision, upSet, ackSet) ->
                        let ackSet' = Set.add ref ackSet
                        if Set.count ackSet' = Map.count upSet then
                            printfn "Received all Acks"
                            match state.crash with
                            | Some (CrashPartialCommit crashSet) ->
                                upSet
                                |> Map.filter (fun id _ -> Set.contains id crashSet)
                                |> Map.iter (fun _ ref -> ref <! "commit")
                                delayExit delay
                            | _ ->
                                ackSet'
                                |> Set.iter (fun ref -> ref <! "commit")
                            match state.master with
                            | Some m ->
                                printfn "Sending a commit"
                                m <! "ack commit"
                            | None -> printfn "WARNING: No master in AckPreCommit"
                            match decision with
                            | Add (name, url) ->
                                sendFullStatetoObservers {
                                    state with
                                        songList = Map.add name url state.songList
                                        commitIter = state.commitIter + 1
                                        commitPhase = CoordCommitted }
                            | Delete name ->
                                sendFullStatetoObservers {
                                    state with
                                        songList = Map.remove name state.songList
                                        commitIter = state.commitIter + 1
                                        commitPhase = CoordCommitted }
                        else
                            printfn "Didn't recieve all Acks yet"
                            { state with commitPhase = CoordCommittable (decision, upSet, ackSet') }
                | CoordCommitted ->
                    printfn "WARNING: Some votes were ignored because they arrived after timeout threshold.\n"
                    state
                | _ ->
                    printfn "WARNING: Invalid commit state in AckPreCommit"
                    state
            return! loop state
        
        | AckPreCommitTimeout sourceIter ->
            printfn "In AckPreCommitTimeout"
            let state' =
                match state.commitPhase with
                | CoordCommittable (decision, upSet, ackSet) ->
                        if Set.count ackSet = Map.count upSet then
                            printfn "WARNING: In AckPreCommitTimeout but have already received all votes."
                            state
                        else
                            // Commit to the processes that have ack'd
                            match state.crash with
                            | Some (CrashPartialCommit crashSet) ->
                                upSet
                                |> Map.filter (fun _ ref -> Set.contains ref ackSet)
                                |> Map.filter (fun id _ -> Set.contains id crashSet)
                                |> Map.iter (fun _ ref -> ref <! "commit")
                                delayExit delay
                            | _ ->
                                ackSet
                                |> Set.iter (fun ref -> ref <! "commit")
                            match state.master with
                            | Some m -> m <! "ack commit"
                            | None -> printfn "WARNING: No master in AckPreCommit"
                            match decision with
                            | Add (name, url) when state.commitIter = sourceIter ->
                                sendFullStatetoObservers {
                                    state with
                                        songList = Map.add name url state.songList
                                        commitIter = state.commitIter + 1
                                        commitPhase = CoordCommitted }
                            | Delete name when state.commitIter = sourceIter ->
                                sendFullStatetoObservers {
                                    state with
                                        songList = Map.remove name state.songList
                                        commitIter = state.commitIter + 1
                                        commitPhase = CoordCommitted }
                            | _ ->
                                printfn "Received an AckPreCommitTimeout in a later iteration"
                                state
                | _ ->
                    printfn "Warning: Invalid state in AckPreCommitTimeout"
                    state
            return! loop state'
        
        | StateReqReply (ref, participantState) ->
            printfn "Received a StateReqReply"
            // Check own decision
            let state' =
                match state.commitPhase with
                | CoordInitCommit (update,upSet,voteSet) ->
                    match participantState with
                    | Aborted ->
                        voteSet
                        |> Set.filter (fun r -> r <> ref)
                        |> Set.iter (fun ref -> ref <! "abort")
                        match state.master with
                        | Some m ->
                            m <! sprintf "coordinator %s" selfID
                            m <! "ack abort"
                        | None -> printfn "WARNING: No master in VoteReq of StateReqReply "
                        sendFullStatetoObservers {
                            state with
                                commitPhase = CoordAborted
                                commitIter = state.commitIter + 1 }
                    | Uncertain ->
                        { state with commitPhase = CoordInitCommit(update, upSet, (Set.add ref voteSet)) }
                    | Committable ->
                        voteSet
                        |> Set.iter (fun ref -> ref <! "precommit")
                        { state with commitPhase = CoordCommittable(update, upSet, Set.empty) }
                    | Committed ->
                        printfn "ERROR: Received Committed while coord is uncertain "
                        state
                | CoordCommittable (update,upSet,ackSet) ->
                    match participantState with
                    | Aborted ->
                        printfn "ERROR: Received Aborted while coord is in committable"
                        state
                    | Uncertain ->
                        ref <! "precommit"
                        state
                    | Committable ->
                        { state with commitPhase = CoordCommittable(update, upSet, (Set.add ref ackSet)) }
                    | Committed ->
                        ackSet
                        |> Set.iter (fun ref -> ref <! "commit")
                        match state.master with
                        | Some m ->
                            printfn "Sending a commit"
                            m <! sprintf "coordinator %s" selfID
                            m <! "ack commit"
                        | None -> printfn "WARNING: No master in AckPreCommit of StateReqReply"
                        match update with
                        | Add (name, url) ->
                            sendFullStatetoObservers 
                                { state with
                                    songList = Map.add name url state.songList
                                    commitIter = state.commitIter + 1
                                    commitPhase = CoordCommitted}
                        | Delete name ->
                            sendFullStatetoObservers 
                                { state with
                                    songList = Map.remove name state.songList
                                    commitIter = state.commitIter + 1
                                    commitPhase = CoordCommitted}
                | CoordCommitted ->
                    match participantState with
                    | Committable -> ref <! "commit"
                    | Committed -> ()
                    | _ -> printfn "WARNING: Incompatible state in CoordCommitted of StateReqReply"
                    state
                | CoordAborted ->
                    match participantState with
                    | Uncertain -> ref <! "abort"
                    | Aborted -> ()
                    | _ -> printfn "WARNING: Incompatible state in CoordAborted of StateReqReply"
                    state
                | _ ->
                    printfn "ERROR: Incompatible state in StateReqReply"
                    state
            return! loop state'

        | StateReqReplyTimeout commitIter ->
            printfn "Received a StateReqReplyTimeout"
            let state' =
                if state.commitIter = commitIter then
                    match state.commitPhase with 
                    | CoordInitCommit (update,upSet,voteSet) ->
                        voteSet
                        |> Set.iter (fun ref -> ref <! "abort")
                        match state.master with
                        | Some m ->
                            m <! sprintf "coordinator %s" selfID
                            m <! "ack abort"
                        | None -> printfn "ERROR: No master in VoteReq of StateReqReplyTimeout "
                        sendFullStatetoObservers {
                            state with
                                commitPhase = CoordAborted
                                commitIter = state.commitIter + 1 }
                    | CoordCommittable (update,upSet,ackSet) ->
                        ackSet
                        |> Set.iter (fun ref -> ref <! "commit")
                        match state.master with
                        | Some m ->
                            printfn "Sending a commit"
                            m <! sprintf "coordinator %s" selfID
                            m <! "ack commit"
                        | None -> printfn "ERROR: No master in AckPreCommit of StateReqReply"
                        match update with
                        | Add (name, url) ->
                            sendFullStatetoObservers { 
                                state with
                                    songList = Map.add name url state.songList
                                    commitIter = state.commitIter + 1
                                    commitPhase = CoordCommitted}
                        | Delete name ->
                            sendFullStatetoObservers {
                                state with
                                    songList = Map.remove name state.songList
                                    commitIter = state.commitIter + 1
                                    commitPhase = CoordCommitted}
                    | _ -> 
                        printfn "ERROR: Unexpected state in StateReqReplyTimeout"
                        state
                else
                    state
            return! loop state'
        
        // Participant side of the protocol
        
        | VoteReq update ->
            
            match state.crash with
            | Some CrashBeforeVote -> delayExit 0
            | _ -> ()

            printfn "Received VoteReq on iteration %i" state.commitIter
            let state =
                startParticipantHeartbeat state
            let upSet =
                getAliveMap state
            // TODO: should votereq contain the coordinator ref?
            // Decide vote according to the rule
            let vote =
                match update with
                | Add (_, url) ->
                    not (String.length url > int selfID + 5)
                | Delete _ ->
                    true
            
            printfn "Voted %s" (if vote then "yes" else "no")
            // Reply to the coordinator with the vote
            match state.coordinator with
            | Some c -> c <! sprintf "votereply %s" (if vote then "yes" else "no")
            | None -> printfn "ERROR: No coordinator in VoteReq"

            match state.crash with
            | Some CrashAfterVote -> delayExit delay
            | _ -> ()

            let state' =
                if vote then
                    // Wait for precommit or timeout
                    setTimeout <| PreCommitTimeout state.commitIter
                    { state with
                        commitPhase = ParticipantInitCommit (update, upSet) }
                else
                    // Vote no
                    setTimeout <| ObserverCheckCoordinator (state.commitIter + 1)
                    { state with
                            commitPhase = ParticipantAborted
                            commitIter = state.commitIter + 1}
            // Start heartbeating as a participant
            return! loop state'

        | PreCommit ->
            printfn "Received PreCommit"
            match state.coordinator with
            | Some c ->
                c <! "ackprecommit"
            | None ->
                printfn "ERROR: No Coordinator in PreCommit"

            match state.crash with
            | Some CrashAfterAck -> delayExit delay
            | _ -> ()

            let state' =
                match state.commitPhase with
                | ParticipantInitCommit (update, upSet) ->            
                    // Wait for commit or timeout
                    setTimeout <| CommitTimeout state.commitIter
                    { state with
                            commitPhase = ParticipantCommittable (update, upSet)}
                | ParticipantCommittable _ ->
                    printfn "WARNING: Received a duplicate precommit in PreCommit"
                    state
                | _ ->
                    printfn "ERROR: Invalid commit state in Precommit"
                    state
            return! loop state'
        
        | PreCommitTimeout sourceIter ->
            printfn "In PreCommitTimeout from iteration %i" sourceIter
            let state' =
                // The coordinator may have died
                match state.coordinator with
                | Some c -> 
                    let isCoordinatorDead =
                        getAliveMap state
                        |> Map.exists (fun id ref -> ref = c)
                        |> not
                    if isCoordinatorDead then
                        printfn "Detected Dead Coordinator"
                        initiateElectionProtocol state
                    else
                        printfn "Detected that coordinator still alive %O" state.coordinator
                        match state.commitPhase with
                        | ParticipantInitCommit _ ->
                            setTimeout <| PreCommitTimeout sourceIter
                            state
                        | _ ->
                            state
                | None ->
                    printfn "WARNING: Received a VoteRequest without a coordinator"
                    state
            return! loop state'

        | Decision decision ->
            printfn "In Decision"
            let state' =
                match decision with
                | Abort ->
                    match state.commitPhase with
                    | ParticipantAborted ->
                        printfn "WARNING: Duplicate abort received"
                        state
                    | _ ->
                        printfn "In Decision -> Abort"
                        { state with
                                commitPhase = ParticipantAborted
                                commitIter = state.commitIter + 1 }
                | Commit ->
                    match state.commitPhase with
                    | ParticipantCommittable (Add (name, url), upSet) ->
                        { state with
                                songList = Map.add name url state.songList
                                commitIter = state.commitIter + 1
                                commitPhase = ParticipantCommitted }
                    | ParticipantCommittable (Delete name, upSet) ->
                        { state with
                                songList = Map.remove name state.songList
                                commitIter = state.commitIter + 1
                                commitPhase = ParticipantCommitted }
                    | ParticipantCommitted ->
                        printfn "WARNING: Duplicate commit received"
                        state
                    | _ ->
                        printfn "Invalid commit state in Decision: %O" state.commitPhase
                        state
             
            return! loop state'

        | CommitTimeout sourceIter ->
            printfn "Received a CommitTimeout of sourceIter %i in commitIter %i in phase: %O" sourceIter state.commitIter state.commitPhase
            let state' =
                if state.commitIter = sourceIter then
                    initiateElectionProtocol state
                else if state.commitIter = sourceIter + 1 && state.commitPhase = ParticipantCommitted then
                    // Moved on to the next iteration
                    // Check if coordinator has died after sending us a commit
                    setTimeout <| ObserverCheckCoordinator state.commitIter
                    startObserverHeartbeat { state with commitPhase = Start }
                else
                    state
            return! loop state'
        
        | ObserverCheckCoordinator sourceiter ->
            //printfn "Received ObserverCheckCoordinator while in phase: %O" state.commitPhase
            let state =
                match state.commitPhase, state.coordinator with
                | ParticipantAborted, Some c when state.commitIter = sourceiter ->
                    let isCoordinatorAlive =
                        getAliveMap state
                        |> Map.exists (fun id ref -> ref = c)
                    if isCoordinatorAlive then
                        startObserverHeartbeat { state with commitPhase = Start }
                    else 
                        state
                | Start, Some c when sourceiter = state.commitIter ->
                    let isCoordinatorDead =
                        getAliveMap state
                        |> Map.exists (fun id ref -> ref = c)
                        |> not
                    if isCoordinatorDead then
                        initiateObserverElectionProtocol state
                    else
                        setTimeout <| ObserverCheckCoordinator state.commitIter
                        state
                | Start, None ->
                    let is3PC =
                        state.beatmap
                        |> Map.filter (fun _ (memb, _, lastMs) -> (memb = Participant) && (lastMs < aliveThreshold))
                        |> Map.isEmpty
                        |> not
                    if not is3PC then
                        initiateObserverElectionProtocol state
                    else
                        setTimeout <| ObserverCheckCoordinator state.commitIter
                        state
                | _ ->
                    state
            return! loop state
       

        | StateReq ref ->
            match state.commitPhase with
            | ParticipantInitCommit _ ->
                ref <! "statereqreply uncertain"
            | ParticipantCommittable _ ->
                ref <! "statereqreply committable"
            | ParticipantCommitted _ ->
                ref <! "statereqreply committed"
            | ParticipantAborted ->
                ref <! "statereqreply aborted"
            | s ->
                printfn "ERROR: Invalid commit phase in StateReq - %O" s
            
            // Set timeout
            setTimeout <| StateReqTimeout state.commitIter
            
            return! loop state
        
        | StateReqTimeout commitIter ->
            let state' =
                if state.commitIter = commitIter then
                    match state.coordinator with
                    | Some c ->
                        let isCoordinatorAlive =
                            state
                            |> getAliveMap
                            |> Map.exists (fun _ ref -> ref = c)
                        if isCoordinatorAlive then
                            state
                        else
                            // Try again
                            initiateElectionProtocol state
                    | None ->
                        printfn "ERROR: No coordinator set in StateReqTimeout"
                        state
                else
                    state
            return! loop state'

        | GetSong name ->
            printfn "In GetSong"
            let url =
                state.songList
                |> Map.tryFind name
                |> Option.defaultValue "NONE"

            match state.master with
            | Some m -> m <! (sprintf "resp %s" url)
            | None -> printfn "WARNING: No master in GetSong"
            
            return! loop state

        | Leave ref ->
            return! loop { state with actors = Set.remove ref state.actors }
        
        | SetCrashAfterVote ->
            return! loop { state with crash = Some CrashAfterVote }
        
        | SetCrashBeforeVote ->
            return! loop { state with crash = Some CrashBeforeVote }
        
        | SetCrashAfterAck ->
            return! loop { state with crash = Some CrashAfterAck }
        
        | SetCrashVoteReq crashSet ->
            return! loop { state with crash = Some (CrashVoteReq crashSet) }
        
        | SetCrashPartialPreCommit crashSet ->
            return! loop { state with crash = Some (CrashPartialPreCommit crashSet) }
        
        | SetCrashPartialCommit crashSet ->
            return! loop { state with crash = Some (CrashPartialCommit crashSet) }
    
    }

    // Concurrently, try to determine whether a coordinator exists
    scheduleOnce mailbox 3000L mailbox.Self DetermineCoordinator
    |> ignore

    loop {
        actors = Set.empty ;
        coordinator = None ;
        master = None ;
        beatmap = Map.empty ;
        songList = Map.empty ;
        commitPhase = Start ;
        commitIter = 0 ;
        beatCancels = [] ;
        crash = None }