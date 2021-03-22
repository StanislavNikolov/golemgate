package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"

	mc "github.com/Tnze/go-mc/net"
	pk "github.com/Tnze/go-mc/net/packet"
)

/*
import (
        "database/sql"
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
func setupDB() {
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
}

// From database
func getRoutingInfo(vhost string) (int, error) {
        var port int
        err := DB.QueryRow("SELECT port FROM routing WHERE vhost=$1", vhost).Scan(&port)
        if err != nil {
                return 0, errors.New("No such vhost")
        }

        return port, nil
}
*/

// TODO extract parseJson and getRoutingInfo in another package
func parseJson(readable io.Reader) (map[string]interface{}, error) {
	var resp map[string]interface{}

	rawRespBytes, err := io.ReadAll(readable)
	if err != nil {
		return resp, errors.New("failed to read readable")
	}

	err = json.Unmarshal(rawRespBytes, &resp)
	if err != nil {
		return resp, errors.New("failed to failed to parse body")
	}

	return resp, nil
}

func getRoutingInfo(vhost string, requestStart bool) (int, error) {
	vhost = strings.Split(vhost, ".")[0]
	url := fmt.Sprintf("http://localhost:8000/api/v1/mcroute/%s", vhost)

	var rawResp *http.Response
	var err error
	if requestStart {
		rawResp, err = http.Post(url, "", bytes.NewBuffer([]byte{}))
	} else {
		rawResp, err = http.Get(url)
	}

	if err != nil {
		return 0, errors.New("failed to contact central API")
	}
	resp, err := parseJson(rawResp.Body)

	if err != nil {
		fmt.Println(resp["error"])
		return 0, errors.New("error parsing response by central API")
	}

	if resp["error"] == "World not loaded" {
		return 0, errors.New("world not loaded")
	}

	if port, ok := resp["port"].(float64); ok {
		// type assertion succeeded and port can be asserted to type int
		return int(port), nil
	}

	return 0, errors.New("wrong type of port in JSON")
}

func generateMCStatusJSON(currentlyOnline int, maxOnline int, description string) string {
	// TODO get as parameters
	const protocolId = 754
	const versionName = "1.16.5"

	return fmt.Sprintf(`{
        "version": { "name": "%s", "protocol": %d },
        "players": { "max": %d, "online": %d, "sample": [] },
        "description": { "text": "%s" }
        }`, versionName, protocolId, maxOnline, currentlyOnline, description)
}

func handshake(conn mc.Conn) (pk.Packet, uint16, string, error) {
	var (
		Protocol, Intention pk.VarInt // ignored
		ServerAddress       pk.String
		ServerPort          pk.UnsignedShort // ignored
	)

	packet, err := conn.ReadPacket()
	if err != nil {
		return packet, 0, "", err
	}

	err = packet.Scan(&Protocol, &ServerAddress, &ServerPort, &Intention)
	return packet, uint16(Intention), string(ServerAddress), err
}

func bridgeConnection(conn mc.Conn, port int, capturedPacket pk.Packet) {
	// open a socket to the target
	server, err := net.Dial("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Println("Failed to connect to server on port", port) // TODO important, log it
		return
	}
	defer server.Close()

	// send the handshake packet we captured earlier
	server.Write(capturedPacket.Pack(0))

	// now stop meddling with the data, just pass it through
	fmt.Println("Bridging", conn.Socket.RemoteAddr())
	var pipe = func(src net.Conn, dst net.Conn, wg *sync.WaitGroup) {
		defer wg.Done()

		buff := make([]byte, 1024)
		for {
			n, err := src.Read(buff)
			if err != nil {
				return
			}

			_, err = dst.Write(buff[:n])
			if err != nil {
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

func handleConnection(conn mc.Conn) {
	defer conn.Close()
	fmt.Println(conn.Socket.RemoteAddr(), "connected")

	// get enough information from the first MC packet to decide how to route
	handshakePacket, intention, vhost, err := handshake(conn)
	if err != nil {
		fmt.Println("Failed to read first packet")
		return
	}

	if intention == 1 { // status packet
		// just a ping, no need to start any server
		port, err := getRoutingInfo(vhost, false)
		if err != nil {
			if err.Error() == "world not loaded" {
				// This is fine, just reply with 0 players online
				//description := fmt.Sprintf("Admin panel at playopenmc.com/admin/%s", vhost)
				statusJSON := generateMCStatusJSON(0, 4, "Waiting for you <3")
				conn.WritePacket(pk.Marshal(0x00, pk.String(statusJSON)))
				return
			}
			// something else happended, we cannot recover
			fmt.Println("Unrecoverable error with handleConnection:", err)
			return
		}

		// looks like the server is already started. Forward the request to it
		// because only the server knows how many players are online right now
		bridgeConnection(conn, port, handshakePacket)
	} else { // intention == 2; login packet
		port, err := getRoutingInfo(vhost, true)
		if err != nil {
			fmt.Println("Unrecoverable error with handleConnection:", err)
			return
		}
		fmt.Println("Route:", conn.Socket.RemoteAddr(), "->", port, vhost)
		bridgeConnection(conn, port, handshakePacket)
	}
}

func main() {
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
