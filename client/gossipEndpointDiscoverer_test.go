package client_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/stretchr/testify/require"
)

func TestGossipEndPointDiscoverer_Unavailable(t *testing.T) {
	seed, _ := url.Parse("http://127.0.0.1:2113")
	discoverer := client.NewGossipEndpointDiscoverer()
	discoverer.MaxDiscoverAttempts = 10
	discoverer.GossipSeeds = []*url.URL{seed}
	member, _ := discoverer.Discover()
	require.Nil(t, member)
}

func TestGossipEndPointDiscoverer_WithNoAliveNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		gossipResponse := `
        {
			"members": [
				{
				"state": "Manager",
				"isAlive": false,
				"externalTcpIp": "127.0.0.3",
				"externalTcpPort": 3112,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.3",
				"externalHttpPort": 3112
				},
				{
				"state": "Follower",
				"isAlive": false,
				"externalTcpIp": "127.0.0.2",
				"externalTcpPort": 2114,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.2",
				"externalHttpPort": 2113
				},
				{
				"state": "Leader",
				"isAlive": false,
				"externalTcpIp": "127.0.0.1",
				"externalTcpPort": 1114,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.1",
				"externalHttpPort": 1113
				}
			],
			"serverIp": "127.0.0.1",
			"serverPort": 1112
		}`
		fmt.Fprintln(w, gossipResponse)
	}))
	defer server.Close()

	serverUrl := server.URL
	seed, _ := url.Parse(serverUrl)
	discoverer := client.NewGossipEndpointDiscoverer()
	discoverer.MaxDiscoverAttempts = 10
	discoverer.GossipSeeds = []*url.URL{seed}
	member, err := discoverer.Discover()
	if err != nil {
		t.Fatalf("Discover Failed")
	}
	require.Nil(t, member, "Expected member to be nil")
}

func TestGossipEndPointDiscoverer_WithAliveLeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		gossipResponse := `
        {
			"members": [
				{
				"state": "Manager",
				"isAlive": false,
				"externalTcpIp": "127.0.0.3",
				"externalTcpPort": 3112,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.3",
				"externalHttpPort": 3112
				},
				{
				"state": "Follower",
				"isAlive": true,
				"externalTcpIp": "127.0.0.2",
				"externalTcpPort": 2114,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.2",
				"externalHttpPort": 2113
				},
				{
				"state": "Leader",
				"isAlive": true,
				"externalTcpIp": "127.0.0.1",
				"externalTcpPort": 1114,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.1",
				"externalHttpPort": 1113
				}
			],
			"serverIp": "127.0.0.1",
			"serverPort": 1112
		}`
		fmt.Fprintln(w, gossipResponse)
	}))
	defer server.Close()

	serverUrl := server.URL
	seed, _ := url.Parse(serverUrl)
	discoverer := client.NewGossipEndpointDiscoverer()
	discoverer.MaxDiscoverAttempts = 10
	discoverer.GossipSeeds = []*url.URL{seed}
	discoverer.NodePreference = client.NodePreference_Leader
	member, err := discoverer.Discover()
	if err != nil {
		t.Fatalf("Discover Failed")
	}
	if member.State != "Leader" {
		t.Fatalf("Expected State to be Leader but was %s", member.State)
	}
	if member.IsAlive != true {
		t.Fatalf("Expected IsAlive to be true but was %v", member.IsAlive)
	}
}

func TestGossipEndPointDiscoverer_WithAliveLeaderAndFollowerAndNodePreferenceFollower(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		gossipResponse := `
        {
			"members": [
				{
				"state": "Manager",
				"isAlive": false,
				"externalTcpIp": "127.0.0.3",
				"externalTcpPort": 3112,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.3",
				"externalHttpPort": 3112
				},
				{
				"state": "Follower",
				"isAlive": true,
				"externalTcpIp": "127.0.0.2",
				"externalTcpPort": 2114,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.2",
				"externalHttpPort": 2113
				},
				{
				"state": "Leader",
				"isAlive": true,
				"externalTcpIp": "127.0.0.1",
				"externalTcpPort": 1114,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.1",
				"externalHttpPort": 1113
				}
			],
			"serverIp": "127.0.0.1",
			"serverPort": 1112
		}`
		fmt.Fprintln(w, gossipResponse)
	}))
	defer server.Close()

	serverUrl := server.URL
	seed, _ := url.Parse(serverUrl)
	discoverer := client.NewGossipEndpointDiscoverer()
	discoverer.MaxDiscoverAttempts = 10
	discoverer.GossipSeeds = []*url.URL{seed}
	discoverer.NodePreference = client.NodePreference_Follower
	member, err := discoverer.Discover()
	if err != nil {
		t.Fatalf("Discover Failed")
	}
	if member.State != client.Follower {
		t.Fatalf("Expected State to be Follower but was %s", member.State)
	}
	if member.IsAlive != true {
		t.Fatalf("Expected IsAlive to be true but was %v", member.IsAlive)
	}
}
