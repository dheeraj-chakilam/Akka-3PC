module ChatServer.World

open Akka.FSharp
open Akka.Actor

type RoomState = {
    actors: Set<IActorRef>
    master: IActorRef option
    messages: List<string>
    beatmap: Map<string,Member*IActorRef*int64>
}

and Member =
    | Participant
    | Coordinator

type RoomMsg =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | Heartbeat of string * Member * IActorRef * int64
    | Alive of int64 * string
    | Broadcast of string
    | Rebroadcast of string
    | Get
    | VoteReq
    | VoteReply of VoteMsg
    | PreCommit
    | AckPreCommit
    | Decision of DecisionMsg
    | Leave of IActorRef

and DecisionMsg =
    | Abort
    | Commit

and VoteMsg =
    | Yes
    | No

let room selfID beatrate aliveThreshold (mailbox: Actor<RoomMsg>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(System.TimeSpan.FromMilliseconds 0.,
                                            System.TimeSpan.FromMilliseconds beatrate,
                                            ref,
                                            sprintf "heartbeat %s" selfID)
            
            return! loop { state with actors = Set.add ref state.actors }

        | JoinMaster ref ->
            return! loop { state with master = Some ref }

        | Heartbeat (id, memb, ref, lastMs) ->
            //printfn "heartbeat %s" id
            return! loop { state with beatmap = state.beatmap |> Map.add id (memb, ref,lastMs) }

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

        | Get ->
            match state.master with
            | Some m -> m <! (sprintf "messages %s" (System.String.Join(",",List.rev state.messages)))
            | None -> ()
            
            return! loop state

        | Leave ref ->
            return! loop { state with actors = Set.remove ref state.actors }
    }
    loop { actors = Set.empty ; master = None ; messages = []; beatmap = Map.empty }