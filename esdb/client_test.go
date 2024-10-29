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

	populatedContainer, populatedClient := CreatePopulatedDatabase(t)

	if populatedContainer != nil {
		defer populatedContainer.Close()
	}

	if populatedClient != nil {
		defer populatedClient.Close()
	}

	isCluster := GetEnvOrDefault("CLUSTER", "false") == "true"
	isInsecure := GetEnvOrDefault("EVENTSTORE_INSECURE", "true") == "true"

	if isCluster {
		ClusterTests(t)
	}

	AppendTests(t, emptyContainer, emptyClient)
	ReadStreamTests(t, emptyClient, populatedClient)
	SubscriptionTests(t, emptyClient, populatedClient)
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

	populatedContainer, populatedClient := CreatePopulatedDatabase(t)

	if populatedContainer != nil {
		defer populatedContainer.Close()
	}

	if populatedClient != nil {
		defer populatedClient.Close()
	}

	PersistentSubReadTests(t, emptyClient)
	PersistentSubTests(t, emptyClient, populatedClient)
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

func TestPlugins(t *testing.T) {
	isCluster := GetEnvOrDefault("CLUSTER", "false") == "true"

	if !isCluster {
		emptyContainer, emptyClient := CreateEmptyDatabase(t)

		if emptyContainer != nil {
			defer emptyContainer.Close()
		}

		if emptyClient != nil {
			defer emptyClient.Close()
		}

		ClientCertificatesSingleNodeTests(t, emptyContainer)
	} else {
		ClientCertificatesClusterNodesTests(t)
	}
}

func TestExpectations(t *testing.T) {
	populatedContainer, populatedClient := CreatePopulatedDatabase(t)

	if populatedContainer != nil {
		defer populatedContainer.Close()
	}

	if populatedClient != nil {
		defer populatedClient.Close()
	}

	ReadAllTests(t, populatedClient)
}

func TestMisc(t *testing.T) {
	ConnectionStringTests(t)
	TestPositionParsing(t)
	UUIDParsingTests(t)
}

func TestClusterRebalance(t *testing.T) {
	ClusterRebalanceTests(t)
}
