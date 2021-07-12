package client

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type EndPoint struct {
	Host string
	Port uint16
}

func (e *EndPoint) String() string {
	return fmt.Sprintf("%s:%d", e.Host, e.Port)
}

func NewEndPoint(host string, port uint16) EndPoint {
	return EndPoint{
		Host: host,
		Port: port,
	}
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

func CreateGrpcConnection(conf *Configuration, address string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	if conf.DisableTLS {
		opts = append(opts, grpc.WithInsecure())
	} else {
		opts = append(opts,
			grpc.WithTransportCredentials(credentials.NewTLS(
				&tls.Config{
					InsecureSkipVerify: conf.SkipCertificateVerification,
					RootCAs:            conf.RootCAs,
				})))
	}

	opts = append(opts, grpc.WithPerRPCCredentials(basicAuth{
		username: conf.Username,
		password: conf.Password,
	}))

	if conf.KeepAliveInterval >= 0 {
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                conf.KeepAliveInterval,
			Timeout:             conf.KeepAliveTimeout,
			PermitWithoutStream: true,
		}))
	}

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize connection to %+v. Reason: %v", conf, err)
	}

	return conn, nil
}
