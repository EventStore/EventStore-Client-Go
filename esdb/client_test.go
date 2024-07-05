package esdb_test

import (
	"testing"
)

func TestStreams(t *testing.T) {
	emptyContainer, emptyClient := CreateEmptyDatabase(t)

	if emptyContainer != nil {
		defer emptyContainer.Close()
	}

	if emptyClient != nil {
		defer emptyClient.Close()
	}

	isCluster := GetEnvOrDefault("CLUSTER", "false") == "true"
	isInsecure := GetEnvOrDefault("EVENTSTORE_INSECURE", "true") == "true"

	if isCluster {
		ClusterTests(t)
	}

	AppendTests(t, emptyContainer, emptyClient)
	ReadStreamTests(t, emptyClient)
	SubscriptionTests(t, emptyClient)
	DeleteTests(t, emptyClient)
	ConnectionTests(t, emptyContainer)

	if !isCluster {
		if !isInsecure {
			TLSTests(t, emptyContainer)
			SecureAuthenticationTests(t, emptyClient)
		} else {
			InsecureAuthenticationTests(t, emptyClient)
		}
	}
}

func TestPersistentSubscriptions(t *testing.T) {
	emptyContainer, emptyClient := CreateEmptyDatabase(t)

	if emptyContainer != nil {
		defer emptyContainer.Close()
	}

	if emptyClient != nil {
		defer emptyClient.Close()
	}

	PersistentSubReadTests(t, emptyClient)
	PersistentSubTests(t, emptyClient)
}

func TestProjections(t *testing.T) {
	emptyContainer, emptyClient := CreateEmptyDatabase(t)

	if emptyContainer != nil {
		defer emptyContainer.Close()
	}

	if emptyClient != nil {
		defer emptyClient.Close()
	}

	ProjectionTests(t, emptyClient)
}

func TestMisc(t *testing.T) {
	ConnectionStringTests(t)
	TestPositionParsing(t)
	UUIDParsingTests(t)
}
