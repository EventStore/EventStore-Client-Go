package esdb_test

import (
	"testing"
)

func TestSingleNode(t *testing.T) {
	// Empty database container
	t.Log("[debug] starting empty database container...")
	emptyContainer := GetEmptyDatabase(t)
	defer emptyContainer.Close()
	emptyContainerClient := CreateTestClient(emptyContainer, t)
	defer emptyContainerClient.Close()
	WaitForAdminToBeAvailable(t, emptyContainerClient)
	t.Log("[debug] empty database container started and ready to serve!")
	//

	// Prepopulated database container
	t.Log("[debug] starting prepopulated database container...")
	populatedContainer := GetPrePopulatedDatabase(t)
	defer populatedContainer.Close()
	populatedContainerClient := CreateTestClient(populatedContainer, t)
	defer populatedContainerClient.Close()
	WaitForAdminToBeAvailable(t, populatedContainerClient)
	t.Log("[debug] prepopulated database container started and ready to serve!")
	//

	// Those ReadAll tests need to be executed first because those are based on $all specific order.
	ReadAllTests(t, populatedContainerClient)
	ReadStreamTests(t, emptyContainerClient, populatedContainerClient)
	SubscriptionTests(t, emptyContainerClient, populatedContainerClient)
	AppendTests(t, emptyContainer, emptyContainerClient)
	ConnectionTests(t, emptyContainer)
	DeleteTests(t, emptyContainerClient)
	PersistentSubReadTests(t, emptyContainerClient)
	PersistentSubTests(t, emptyContainerClient, populatedContainerClient)
	TLSTests(t, emptyContainer)
}

func TestClusterNode(t *testing.T) {
	db := CreateClient("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?nodepreference=leader&tlsverifycert=false", t)
	defer db.Close()

	WaitForAdminToBeAvailable(t, db)
	WaitForLeaderToBeElected(t, db)

	ClusterTests(t)
	ReadStreamTests(t, db, nil)
	AppendTests(t, nil, db)
	DeleteTests(t, db)
	PersistentSubReadTests(t, db)
	PersistentSubTests(t, db, nil)
}
