module ChatServer.World

open Akka.FSharp
open Akka.Actor

type RoomState = {
    actors: Set<IActorRef>
    coordinator: Option<string * IActorRef>
    master: IActorRef option
    messages: List<string>
    beatmap: Map<string,Member*IActorRef*int64>
    songList: Map<string, string>
}

and Member =
    | Participant
    | Coordinator

type RoomMsg =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | DetermineCoordinator
    | Heartbeat of string * Member * IActorRef * int64
    | AddSong of SongName * Url
    | GetSong of SongName
    | DeleteSong of SongName
    | Alive of int64 * string
    | Broadcast of string
    | Rebroadcast of string
    | VoteReq of UpdateType
    | VoteReply of VoteMsg
    | PreCommit
    | AckPreCommit
    | Decision of DecisionMsg
    | SongList of string
    | Leave of IActorRef

and DecisionMsg =
    | Abort
    | Commit

and VoteMsg =
    | Yes
    | No

and SongName = SongName of string

and Url = Url of string

and UpdateType =
    | Add
    | Delete

let scheduleRepeatedly (mailbox:Actor<_>) rate actorRef message =
    mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(
        System.TimeSpan.FromMilliseconds 0.,
        System.TimeSpan.FromMilliseconds rate,
        actorRef,
        message)

let scheduleOnce (mailbox:Actor<_>) after actorRef message =
    mailbox.Context.System.Scheduler.ScheduleTellOnce(
        System.TimeSpan.FromMilliseconds after,
        actorRef,
        message)

let room selfID beatrate aliveThreshold (mailbox: Actor<RoomMsg>) =
    let rec loop state = actor {
        //let sendToAlive message =
        //    state.beatmap
        //    |> Map.filter (fun _ (_,_, ms) -> currMs - ms < aliveThreshold)

        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            //TODO: Send song list
            //TODO: Make the heartbeats cancellable
            match state.coordinator with
            | None ->
                scheduleRepeatedly mailbox beatrate ref (sprintf "coordinator %s" selfID)
            | Some (id, _) when id = selfID ->
                scheduleRepeatedly mailbox beatrate ref (sprintf "coordinator %s" selfID)
            | Some (id, _) when id <> selfID ->
                scheduleRepeatedly mailbox beatrate ref (sprintf "participant %s" selfID)
            
            return! loop { state with actors = Set.add ref state.actors }

        | JoinMaster ref ->
            return! loop { state with master = Some ref }

        | Heartbeat (id, memb, ref, lastMs) ->
            //printfn "heartbeat %s" id
            return! loop {
                state with
                    beatmap = state.beatmap |> Map.add id (memb, ref, lastMs) ;
                    coordinator = match memb with
                                  | Coordinator -> Some (id, ref)
                                  | Participant -> state.coordinator }

        | DetermineCoordinator ->
            return! loop {
                state with
                    coordinator = match state.coordinator with
                                  | None -> printfn "%s is the coordinator" selfID; Some (selfID, sender) 
                                  | _ -> printfn "%A is the coordinator" state.coordinator; state.coordinator }


        | Alive (currMs, selfID) ->
            match state.master with
            | Some m -> 
                let aliveList =
                    state.beatmap
                    |> Map.filter (fun _ (_,_, ms) -> currMs - ms < aliveThreshold)
                    |> Map.add selfID (Unchecked.defaultof<_>, Unchecked.defaultof<_>, Unchecked.defaultof<_>)
                    |> Map.toList
                    |> List.map (fun (id,_) -> id)
                m <! (sprintf "alive %s" (System.String.Join(",",aliveList)))
            | None -> ()

            return! loop state

        | Broadcast text -> //This message is from the master
            state.actors
            |> Set.iter (fun a -> a <! (sprintf "rebroadcast %s" text))
            
            return! loop { state with messages = text :: state.messages }

        | Rebroadcast text ->
            return! loop { state with messages = text :: state.messages }

        | Get (SongName name) ->
            let url =
                state.songList
                |> Map.tryFind name
                |> Option.defaultValue "NONE"

            match state.master with
            | Some m -> m <! (sprintf "resp %s" url)
            | None -> ()
            
            return! loop state
        
        | SongList string ->
            //TODO: Handle an incoming song list on recovery/late join
            ()

        | Leave ref ->
            return! loop { state with actors = Set.remove ref state.actors }
    }
    
    loop { actors = Set.empty ; coordinator = None; master = None ; messages = []; beatmap = Map.empty; songList = Map.empty }