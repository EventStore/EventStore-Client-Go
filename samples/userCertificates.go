package samples

import (
	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
)

func UserCertificates() {
	// region client-with-user-certificates
	settings, err := kurrent.ParseConnectionString("esdb://admin:changeit@{endpoint}?tls=true&userCertFile={pathToCaFile}&userKeyFile={pathToKeyFile}")

	if err != nil {
		panic(err)
	}

	db, err := kurrent.NewClient(settings)
	// endregion client-with-user-certificates

	if err != nil {
		panic(err)
	}

	db.Close()
}
