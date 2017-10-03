module ChatServer.Main

open Akka.FSharp
open World
open Network

let beatrate = 100.
let aliveThreshold = 1000L

let config =
    (Akka.Configuration.ConfigurationFactory.ParseString
        @"
        akka {
        suppress-json-serializer-warning = on
        }").WithFallback(Configuration.defaultConfig())

[<EntryPoint>]
let main argv =
    match argv with
    | [|id; n; port|] ->
        let system = System.create "system" config
        let roomRef = spawn system "room" (room id beatrate aliveThreshold)
        let serverRef = spawn system "server" (server roomRef ChatServer (20000 + int id) (int id) (int n))
        let mServerRef = spawn system "master-server" (server roomRef MasterServer (int port) (int id) (int n))
        // TODO: change this to a better place
        // If after 1s, the coordinator hasn't been decided, set itself as the coordinator
        system.Scheduler.ScheduleTellOnce(
            System.TimeSpan.FromMilliseconds 3000.,
            roomRef,
            DetermineCoordinator)
        ()

    | _ -> printfn "Incorrect arguments (%A)" argv
    
    System.Console.ReadLine() |> ignore
    0