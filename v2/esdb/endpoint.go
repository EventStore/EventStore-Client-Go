package esdb

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

type EndPoint struct {
	Host string
	Port uint16
}

func (e *EndPoint) String() string {
	return fmt.Sprintf("%s:%d", e.Host, e.Port)
}

func ParseEndPoint(s string) (*EndPoint, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("an empty host is specified")
	}

	endpoint := &EndPoint{}
	if strings.Contains(s, ":") {
		tokens := strings.Split(s, ":")
		if len(tokens) != 2 {
			return nil, fmt.Errorf("too many colons specified in host, expecting {host}:{port}")
		}

		ip := net.ParseIP(tokens[0])
		if ip == nil {
			_, err := url.Parse(tokens[0])

			if err != nil {
				return nil, fmt.Errorf("invalid hostname [%s]", tokens[0])
			}
		}

		port, err := strconv.Atoi(tokens[1])
		if err != nil || !(port >= 1 && port <= 65_535) {
			return nil, fmt.Errorf("invalid port specified, expecting an integer value [%s]", tokens[1])
		}

		endpoint.Host = tokens[0]
		endpoint.Port = uint16(port)
	} else {
		ip := net.ParseIP(s)
		if ip == nil {
			_, err := url.Parse(s)

			if err != nil {
				return nil, fmt.Errorf("invalid hostname [%s]", s)
			}
		}

		endpoint.Host = s
		endpoint.Port = 2_113
	}

	return endpoint, nil
}

func NewGrpcClient(config Configuration) *grpcClient {
	channel := make(chan msg)

	go connectionStateMachine(config, channel)

	return &grpcClient{
		channel: channel,
	}
}
