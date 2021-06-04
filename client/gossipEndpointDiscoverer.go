package client

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"

	log "github.com/sirupsen/logrus"
)

// GossipEndpointDiscoverer used for discovering and picking the most appropriate node in a cluster
type GossipEndpointDiscoverer struct {
	MaxDiscoverAttempts       int
	GossipSeeds               []*url.URL
	NodePreference            NodePreference
	SkipCertificateValidation bool
	httpClient                *http.Client
}

func NewGossipEndpointDiscoverer() *GossipEndpointDiscoverer {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: transport}
	return &GossipEndpointDiscoverer{
		MaxDiscoverAttempts:       10,
		NodePreference:            NodePreference_Leader,
		SkipCertificateValidation: false,
		httpClient:                client,
	}
}

// Discover will discover nodes via performing a gossip over HTTP and then picking the best candidate to connect to
func (discoverer *GossipEndpointDiscoverer) Discover() (*MemberInfo, error) {
	if len(discoverer.GossipSeeds) == 0 {
		return nil, errors.New("There are no gossip seeds")
	}
	discoverer.GossipSeeds = shuffleGossipSeeds(discoverer.GossipSeeds)
	gossipIndex := 0
	for attempt := 1; attempt <= discoverer.MaxDiscoverAttempts; attempt++ {
		if gossipIndex >= len(discoverer.GossipSeeds) {
			gossipIndex = 0
		}
		gossipSeed := discoverer.GossipSeeds[gossipIndex]
		gossipIndex++
		if log.IsLevelEnabled(log.DebugLevel) {
			log.WithField("gossipSeed", gossipSeed).Debug("Attempting to gossip")
		}
		member, err := discoverEndPoint(gossipSeed, discoverer.httpClient, discoverer.NodePreference)
		if err != nil {
			if attempt == discoverer.MaxDiscoverAttempts {
				return nil, fmt.Errorf("Failed to discover any cluster node members via gossip. Maximum number of attempts reached. %s", err.Error())
			}
			continue
		}
		if log.IsLevelEnabled(log.DebugLevel) {
			log.WithField("candidate", member).Debug("Selected candidate")
		}
		return member, nil
	}
	return nil, nil
}

func discoverEndPoint(gossipSeed *url.URL, httpClient *http.Client, nodePreference NodePreference) (*MemberInfo, error) {
	gossipResponse, err := gossip(gossipSeed, httpClient)
	if err != nil {
		return nil, err
	}
	candidate, _ := getBestCandidate(gossipResponse, nodePreference)
	return candidate, nil
}

func shuffleGossipSeeds(src []*url.URL) []*url.URL {
	for i := range src {
		j := rand.Intn(i + 1)
		src[i], src[j] = src[j], src[i]
	}
	return src
}

func sortByNodeState(members []MemberInfo, nodeState NodeState) []MemberInfo {
	sorted := make([]MemberInfo, 0)
	for _, member := range members {
		if member.State == nodeState {
			sorted = append(sorted, member)
		}
	}
	for _, member := range members {
		if member.State != nodeState {
			sorted = append(sorted, member)
		}
	}
	return sorted
}

func getBestCandidate(response GossipResponse, nodePreference NodePreference) (*MemberInfo, error) {
	if len(response.Members) == 0 {
		return nil, errors.New("There are no members to determine the best candidate from")
	}
	allowedMembers := make([]MemberInfo, 0)
	for _, member := range response.Members {
		for _, allowedState := range allowedStates() {
			if member.State == allowedState && member.IsAlive {
				allowedMembers = append(allowedMembers, member)
			}
		}
	}
	if len(allowedMembers) == 0 {
		return nil, fmt.Errorf("No nodes are eligable to be a candidate")
	}
	switch nodePreference {
	case NodePreference_Leader:
		{
			allowedMembers = sortByNodeState(allowedMembers, Leader)
		}
	case NodePreference_Follower:
		{
			allowedMembers = sortByNodeState(allowedMembers, Follower)
		}
	case NodePreference_ReadOnlyReplica:
		{
			allowedMembers = sortByNodeState(allowedMembers, ReadOnlyLeaderless)
			allowedMembers = sortByNodeState(allowedMembers, ReadOnlyReplica)
		}
	}
	return &allowedMembers[0], nil
}

func gossip(gossipSeed *url.URL, httpClient *http.Client) (GossipResponse, error) {
	response, err := httpClient.Get(fmt.Sprintf("http://%s/gossip", gossipSeed.String()))
	if err != nil || response.StatusCode != http.StatusOK {
		return GossipResponse{}, err
	}
	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	var gossipResponse GossipResponse
	err = json.Unmarshal(body, &gossipResponse)
	if err != nil {
		return GossipResponse{}, err
	}
	return gossipResponse, nil
}

func allowedStates() []NodeState {
	return []NodeState{
		Follower,
		Leader,
		ReadOnlyLeaderless,
		ReadOnlyReplica,
	}
}

// GossipSeed represents and endpoint where a gossip can be issued and nodes in a cluster discovered
type GossipSeed struct {
	ExternalTCPIP    string
	ExternalHTTPPort int
}

//GossipResponse represents the response from a gossip request
type GossipResponse struct {
	Members []MemberInfo `json:"members"`
}
