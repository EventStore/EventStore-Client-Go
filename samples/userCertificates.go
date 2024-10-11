package samples

import (
	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
)

func UserCertificates() {
	// region client-with-user-certificates
	settings, err := esdb.ParseConnectionString("esdb://admin:changeit@{endpoint}?tls=true&userCertFile={pathToCaFile}&userKeyFile={pathToKeyFile}")

	if err != nil {
		panic(err)
	}

	db, err := esdb.NewClient(settings)
	// endregion client-with-user-certificates

	if err != nil {
		panic(err)
	}

	db.Close()
}
