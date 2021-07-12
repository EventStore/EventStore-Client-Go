package client

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	gossipApi "github.com/EventStore/EventStore-Client-Go/protos/gossip"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	"google.golang.org/grpc"
)

func allowedNodeState() []gossipApi.MemberInfo_VNodeState {
	return []gossipApi.MemberInfo_VNodeState{
		gossipApi.MemberInfo_Follower,
		gossipApi.MemberInfo_Leader,
		gossipApi.MemberInfo_ReadOnlyLeaderless,
		gossipApi.MemberInfo_PreReadOnlyReplica,
	}
}

func DiscoverNode(conf *Configuration) (*grpc.ClientConn, error) {
	var connection *grpc.ClientConn = nil
	attempt := 1

	if conf.DnsDiscover || len(conf.GossipSeeds) > 0 {
		var candidates []string

		if conf.DnsDiscover {
			candidates = append(candidates, conf.Address)
		} else {
			for _, seed := range conf.GossipSeeds {
				candidates = append(candidates, seed.String())
			}
		}

		shuffleCandidates(candidates)

		for attempt <= conf.MaxDiscoverAttempts {
			log.Printf("[info] discovery attempt %v/%v", attempt, conf.MaxDiscoverAttempts)
			for _, candidate := range candidates {
				log.Printf("[info] Attempting to gossip via %s", candidate)
				connection, err := CreateGrpcConnection(conf, candidate)
				if err != nil {
					log.Printf("[warn] Error when creating a grpc connection for candidate %s", candidate)

					continue
				}

				client := gossipApi.NewGossipClient(connection)
				context, cancel := context.WithTimeout(context.Background(), time.Duration(conf.GossipTimeout)*time.Second)
				defer cancel()
				info, err := client.Read(context, &shared.Empty{})

				if err != nil {
					log.Printf("[warn] Error when reading gossip from candidate %s: %v", candidate, err)
					continue
				}

				info.Members = shuffleMembers(info.Members)
				selected, err := pickBestCandidate(info, conf.NodePreference)

				if err != nil {
					log.Printf("[warn] Eror when picking best candidate out of %s gossip response: %v", candidate, err)
					continue
				}

				selectedAddress := fmt.Sprintf("%s:%d", selected.GetHttpEndPoint().GetAddress(), selected.GetHttpEndPoint().GetPort())

				if candidate != selectedAddress {
					connection, err = CreateGrpcConnection(conf, selectedAddress)

					if err != nil {
						log.Printf("[warn] Error when creating gRPC connection for best candidate %v", err)
						continue
					}
				}

				return connection, nil
			}

			attempt += 1
			time.Sleep(time.Duration(conf.DiscoveryInterval))
		}

		return nil, fmt.Errorf("maximum discovery attempt count reached")

	} else {
		for attempt <= conf.MaxDiscoverAttempts {
			grpcConn, err := CreateGrpcConnection(conf, conf.Address)

			if err == nil {
				connection = grpcConn
				break
			}

			log.Printf("[warn] error when creating a single node connection to %s", conf.Address)

			attempt += 1
			time.Sleep(time.Duration(conf.DiscoveryInterval))
		}

		if connection == nil {
			return nil, fmt.Errorf("unable to connect to single node %s", conf.Address)
		}
	}

	return connection, nil
}

func shuffleCandidates(src []string) []string {
	for i := range src {
		j := rand.Intn(i + 1)
		src[i], src[j] = src[j], src[i]
	}
	return src
}

func shuffleMembers(src []*gossipApi.MemberInfo) []*gossipApi.MemberInfo {
	for i := range src {
		j := rand.Intn(i + 1)
		src[i], src[j] = src[j], src[i]
	}
	return src
}

func sortByState(members []*gossipApi.MemberInfo, nodeState gossipApi.MemberInfo_VNodeState) []*gossipApi.MemberInfo {
	sorted := make([]*gossipApi.MemberInfo, 0)
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

func pickBestCandidate(response *gossipApi.ClusterInfo, nodePreference NodePreference) (*gossipApi.MemberInfo, error) {
	if len(response.Members) == 0 {
		return nil, fmt.Errorf("there are no members to determine the best candidate from")
	}
	allowedMembers := make([]*gossipApi.MemberInfo, 0)
	for _, member := range response.Members {
		for _, allowedState := range allowedNodeState() {
			if member.State == allowedState && member.GetIsAlive() {
				allowedMembers = append(allowedMembers, member)
			}
		}
	}
	if len(allowedMembers) == 0 {
		return nil, fmt.Errorf("no nodes are eligable to be a candidate")
	}
	switch nodePreference {
	case NodePreference_Leader:
		{
			allowedMembers = sortByState(allowedMembers, gossipApi.MemberInfo_Leader)
		}
	case NodePreference_Follower:
		{
			allowedMembers = sortByState(allowedMembers, gossipApi.MemberInfo_Follower)
		}
	case NodePreference_ReadOnlyReplica:
		{
			allowedMembers = sortByState(allowedMembers, gossipApi.MemberInfo_ReadOnlyLeaderless)
			allowedMembers = sortByState(allowedMembers, gossipApi.MemberInfo_ReadOnlyReplica)
		}
	}
	return allowedMembers[0], nil
}
