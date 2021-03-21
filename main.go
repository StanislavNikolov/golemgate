package main

import (
        "fmt"
        "errors"
        "database/sql"
        "net"
        "sync"
        mc "github.com/Tnze/go-mc/net"
        pk "github.com/Tnze/go-mc/net/packet"
        _ "github.com/lib/pq"
)

const (
        host     = "localhost"
        port     = 5432
        user     = "postgres"
        password = "postgres"
        dbname   = "playopenmc"
)

var DB *sql.DB

func getRoutingInfo(vhost string) (int, error) {
        var port int
        err := DB.QueryRow("SELECT port FROM routing WHERE vhost=$1", vhost).Scan(&port)
        if err != nil {
                return 0, errors.New("No such vhost")
        }

        return port, nil
}

func handshake(conn mc.Conn) (pk.Packet, string, error) {
        var (
                Protocol, Intention pk.VarInt        // ignored
                ServerAddress       pk.String
                ServerPort          pk.UnsignedShort // ignored
        )

        packet, err := conn.ReadPacket()
        if err != nil {
                return packet, "", err
        }

        err = packet.Scan(&Protocol, &ServerAddress, &ServerPort, &Intention)
        return packet, string(ServerAddress), err
}

func handleConnection(conn mc.Conn) {
        defer conn.Close()
        fmt.Println(conn.Socket.RemoteAddr(), "connected")

        // get enough information from the first MC packet to decide how to route
        handshakePacket, vhost, err := handshake(conn)
        if err != nil {
                fmt.Println("Failed to read first packet")
                return
        }

        port, err := getRoutingInfo(vhost)
        if err != nil {
                fmt.Println("Failed to route to", vhost)
                return
        }
        fmt.Println("Route:", conn.Socket.RemoteAddr(), "->", port, vhost)

        // route decided. Open a socket to the target
        server, err := net.Dial("tcp", fmt.Sprintf(":%d", port))
        if err != nil {
                fmt.Println("Failed to connect to server on port", port) // TODO important, log it
                return
        }
        defer server.Close()

        // send the handshake packet we captured to the server
        server.Write(handshakePacket.Pack(0))

        // now stop meddling with the data, just pass it through
        fmt.Println("Bridging", conn.Socket.RemoteAddr())
        var pipe = func(src net.Conn, dst net.Conn, wg *sync.WaitGroup) {
                defer wg.Done()

                buff := make([]byte, 1024)
                for {
                        n, err := src.Read(buff)
                        if err != nil {
                                //fmt.Println("src read err:", err)
                                return
                        }

                        _, err = dst.Write(buff[:n])
                        if err != nil {
                                //fmt.Println("dst read err:", err)
                                return
                        }
                }
        }


        var wg sync.WaitGroup
        wg.Add(2)
        go pipe(conn.Socket, server, &wg)
        go pipe(server, conn.Socket, &wg)
        wg.Wait()
}

func main() {
        psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
                host, port, user, password, dbname)

        db, err := sql.Open("postgres", psqlInfo)
        if err != nil {
                panic(err)
        }
        defer db.Close()

        // actually checks that the connection to the DB is working
        err = db.Ping()
        if err != nil {
                panic(err)
        }

        DB = db

        fmt.Println("Starting server on port 25565")
        ln, err := mc.ListenMC(":25565")
        if err != nil {
                panic(err)
        }

        for {
                conn, err := ln.Accept()
                if err != nil {
                        // TODO: what can raise the error? How can we drop that connection only
                        panic(err)
                }
                go handleConnection(conn)
        }
}
