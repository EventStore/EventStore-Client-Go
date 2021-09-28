package connection_integration_test

//
//func TestTLSDefaults(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	client, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = client.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.Error(t, err)
//	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
//}

//func TestTLSDefaultsWithCertificate(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	b, err := ioutil.ReadFile("../certs/node/node.crt")
//	if err != nil {
//		t.Fatalf("failed to read node certificate ../certs/node/node.crt: %s", err.Error())
//	}
//	cp := x509.NewCertPool()
//	if !cp.AppendCertsFromPEM(b) {
//		t.Fatalf("failed to append node certificates: %s", err.Error())
//	}
//	config.RootCAs = cp
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.NoError(t, err)
//}
//
//func TestTLSWithoutCertificateAndVerify(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=false", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.NoError(t, err)
//}
//
//func TestTLSWithoutCertificate(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.Error(t, err)
//	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
//}
//
//func TestTLSWithCertificate(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	b, err := ioutil.ReadFile("../certs/node/node.crt")
//	if err != nil {
//		t.Fatalf("failed to read node certificate ../certs/node/node.crt: %s", err.Error())
//	}
//	cp := x509.NewCertPool()
//	if !cp.AppendCertsFromPEM(b) {
//		t.Fatalf("failed to append node certificates: %s", err.Error())
//	}
//	config.RootCAs = cp
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.NoError(t, err)
//}
//
//func TestTLSWithCertificateFromAbsoluteFile(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	absPath, err := filepath.Abs("../certs/node/node.crt")
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	s := fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true&tlsCAFile=%s", container.Endpoint, absPath)
//	config, err := client.ParseConnectionString(s)
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.NoError(t, err)
//}
//
//func TestTLSWithCertificateFromRelativeFile(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true&tlsCAFile=../certs/node/node.crt", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.NoError(t, err)
//}
//
//func TestTLSWithInvalidCertificate(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	b, err := ioutil.ReadFile("../certs/untrusted-ca/ca.crt")
//	if err != nil {
//		t.Fatalf("failed to read node certificate ../certs/untrusted-ca/ca.crt: %s", err.Error())
//	}
//	cp := x509.NewCertPool()
//	if !cp.AppendCertsFromPEM(b) {
//		t.Fatalf("failed to append node certificates: %s", err.Error())
//	}
//	config.RootCAs = cp
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.Error(t, err)
//	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
//}
