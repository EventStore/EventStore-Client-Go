package esdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func (client *Client) httpListAllPersistentSubscriptions(options ListPersistentSubscriptionsOptions) ([]PersistentSubscriptionInfo, error) {
	body, err := client.httpExecute("GET", "/subscriptions", options.Authenticated, nil)

	if err != nil {
		return nil, err
	}

	var subs []persistentSubscriptionInfoHttpJson

	err = json.Unmarshal(body, &subs)

	if err != nil {
		return nil, &Error{code: ErrorCodeParsing, err: fmt.Errorf("error when parsing JSON payload: %w", err)}
	}

	var infos []PersistentSubscriptionInfo

	for _, src := range subs {
		info, err := fromHttpJsonInfo(src)
		if err != nil {
			return nil, err
		}

		infos = append(infos, *info)
	}

	return infos, nil
}

func (client *Client) httpListPersistentSubscriptionsForStream(streamName string, options ListPersistentSubscriptionsOptions) ([]PersistentSubscriptionInfo, error) {
	body, err := client.httpExecute("GET", fmt.Sprintf("/subscriptions/%s", url.PathEscape(streamName)), options.Authenticated, nil)

	if err != nil {
		return nil, err
	}

	var subs []persistentSubscriptionInfoHttpJson

	err = json.Unmarshal(body, &subs)

	if err != nil {
		return nil, &Error{code: ErrorCodeParsing, err: fmt.Errorf("error when parsing JSON payload: %w", err)}
	}

	var infos []PersistentSubscriptionInfo

	for _, src := range subs {
		info, err := fromHttpJsonInfo(src)
		if err != nil {
			return nil, err
		}

		infos = append(infos, *info)
	}

	return infos, nil
}

func (client *Client) httpGetPersistentSubscriptionInfo(streamName string, groupName string, options GetPersistentSubscriptionOptions) (*PersistentSubscriptionInfo, error) {
	body, err := client.httpExecute("GET", fmt.Sprintf("/subscriptions/%s/%s/info", url.PathEscape(streamName), url.PathEscape(groupName)), options.Authenticated, nil)

	if err != nil {
		return nil, err
	}

	var src persistentSubscriptionInfoHttpJson

	err = json.Unmarshal(body, &src)

	if err != nil {
		return nil, &Error{code: ErrorCodeParsing, err: fmt.Errorf("error when parsing JSON payload: %w", err)}
	}

	info, err := fromHttpJsonInfo(src)

	if err != nil {
		return nil, err
	}

	return info, nil
}

func (client *Client) httpReplayParkedMessages(streamName string, groupName string, options ReplayParkedMessagesOptions) error {
	params := &httpParams{
		headers: []keyvalue{newKV("content-length", "0")},
	}

	if options.StopAt != 0 {
		params.queries = append(params.queries, newKV("stopAt", strconv.Itoa(options.StopAt)))
	}

	urlStr := fmt.Sprintf("/subscriptions/%s/%s/replayParked", url.PathEscape(streamName), url.PathEscape(groupName))
	_, err := client.httpExecute("POST", urlStr, options.Authenticated, params)

	return err
}

func (client *Client) httpRestartSubsystem(options RestartPersistentSubscriptionSubsystemOptions) error {
	params := &httpParams{
		headers: []keyvalue{newKV("content-length", "0")},
	}

	_, err := client.httpExecute("POST", "/subscriptions/restart", options.Authenticated, params)

	return err
}

func (client *Client) getBaseUrl() (string, error) {
	handle, err := client.grpcClient.getConnectionHandle()

	if err != nil {
		return "", err
	}

	var protocol string
	if client.config.DisableTLS {
		protocol = "http"
	} else {
		protocol = "https"
	}

	return fmt.Sprintf("%s://%s", protocol, handle.connection.Target()), nil
}

type keyvalue struct {
	key   string
	value string
}

func newKV(key string, value string) keyvalue {
	return keyvalue{
		key:   key,
		value: value,
	}
}

type httpParams struct {
	queries []keyvalue
	headers []keyvalue
}

func (client *Client) httpExecute(method string, path string, auth *Credentials, params *httpParams) ([]byte, error) {
	baseUrl, err := client.getBaseUrl()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}

	req, err := http.NewRequest(method, fmt.Sprintf("%s%s", baseUrl, path), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("content-type", "application/json")

	if params != nil {
		if params.headers != nil {
			for i := range params.headers {
				tuple := params.headers[i]
				req.Header.Add(tuple.key, tuple.value)
			}
		}

		if params.queries != nil {
			query := req.URL.Query()
			for i := range params.queries {
				tuple := params.queries[i]
				query.Add(tuple.key, tuple.value)
			}
			req.URL.RawQuery = query.Encode()
		}
	}

	var creds *Credentials
	if auth != nil {
		creds = auth
	} else {
		if client.config.Username != "" {
			creds = &Credentials{
				Login:    client.config.Username,
				Password: client.config.Password,
			}
		}
	}

	if creds != nil {
		req.SetBasicAuth(creds.Login, creds.Password)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 600 {
		switch resp.StatusCode {
		case 401:
			return nil, &Error{code: ErrorCodeAccessDenied}
		case 404:
			return nil, &Error{code: ErrorCodeResourceNotFound}
		default:
			{
				if resp.StatusCode >= 500 && resp.StatusCode < 600 {
					return nil, &Error{code: ErrorCodeInternalServer, err: fmt.Errorf("server returned a '%v' response", resp.StatusCode)}
				}

				return nil, &Error{code: ErrorCodeInternalClient, err: fmt.Errorf("unexpected response code '%v'", resp.StatusCode)}
			}
		}
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, &Error{code: ErrorCodeUnknown, err: err}
	}

	return body, nil
}

func parsePosition(input string) (*Position, error) {
	commitIdx := strings.Index(input, "C:")
	prepareIdx := strings.Index(input, "/P:")

	if commitIdx != 0 || prepareIdx == -1 {
		return nil, &Error{
			code: ErrorCodeParsing,
			err:  fmt.Errorf("error when parsing a position string representation: '%s'", input),
		}
	}

	commit, err := strconv.Atoi(input[2:prepareIdx])
	if err != nil {
		return nil, &Error{
			code: ErrorCodeParsing,
			err:  fmt.Errorf("error when parsing commit position: %w", err),
		}
	}

	prepare, err := strconv.Atoi(input[prepareIdx+3:])
	if err != nil {
		return nil, &Error{
			code: ErrorCodeParsing,
			err:  fmt.Errorf("error when parsing prepare position: %w", err),
		}
	}

	return &Position{Commit: uint64(commit), Prepare: uint64(prepare)}, nil
}

func parseRevisionOrPosition(input string) (interface{}, error) {
	if value, err := strconv.Atoi(input); err == nil {
		return Revision(uint64(value)), nil
	}

	if value, err := parsePosition(input); err == nil {
		return value, nil
	}

	return nil, &Error{
		code: ErrorCodeParsing,
		err:  fmt.Errorf("error when trying to parse a stream revision or position"),
	}
}

func parseEventRevision(input string) (uint64, error) {
	value, err := strconv.Atoi(input)

	if err == nil {
		return uint64(value), nil
	}

	return 0, &Error{
		code: ErrorCodeParsing,
		err:  err,
	}
}

func parseStreamPosition(input string) (interface{}, error) {
	if input == "0" || input == "C:0/P:0" {
		return Start{}, nil
	}
	if input == "-1" || input == "C:-1/P:-1" {
		return End{}, nil
	}

	return parseRevisionOrPosition(input)
}

func fromHttpJsonInfo(src persistentSubscriptionInfoHttpJson) (*PersistentSubscriptionInfo, error) {
	var settings *PersistentSubscriptionSettings
	var stats *PersistentSubscriptionStats
	info := PersistentSubscriptionInfo{}

	if src.Config != nil {
		settings = &PersistentSubscriptionSettings{}
		settings.ResolveLinkTos = src.Config.ResolveLinkTos
		settings.ExtraStatistics = src.Config.ExtraStatistics
		settings.MessageTimeout = int32(src.Config.MessageTimeout)
		settings.MaxRetryCount = int32(src.Config.MaxRetryCount)
		settings.LiveBufferSize = int32(src.Config.LiveBufferSize)
		settings.ReadBatchSize = int32(src.Config.ReadBatchSize)
		settings.CheckpointAfter = int32(src.Config.CheckpointAfter)
		settings.CheckpointLowerBound = int32(src.Config.CheckpointLowerBound)
		settings.CheckpointUpperBound = int32(src.Config.CheckpointUpperBound)
		settings.MaxSubscriberCount = int32(src.Config.MaxSubscriberCount)
		settings.ConsumerStrategyName = ConsumerStrategy(src.Config.ConsumerStrategyName)

		if src.EventStreamId == "$all" {
			from, err := parseStreamPosition(src.Config.StartPosition)

			if err != nil {
				return nil, err
			}
			settings.StartFrom = from
		} else {
			settings.StartFrom = Revision(uint64(src.Config.StartFrom))
		}

		info.Settings = settings

		if src.Config.ExtraStatistics {
			stats = &PersistentSubscriptionStats{}
			stats.AveragePerSecond = int64(src.AverageItemsPerSecond)
			stats.TotalItems = src.TotalItemsProcessed
			stats.CountSinceLastMeasurement = src.CountSinceLastMeasurement
			stats.ReadBufferCount = src.ReadBufferCount
			stats.LiveBufferCount = src.LiveBufferCount
			stats.RetryBufferCount = src.RetryBufferCount
			stats.TotalInFlightMessages = src.TotalInFlightMessages
			stats.OutstandingMessagesCount = src.OutstandingMessagesCount
			stats.ParkedMessagesCount = src.ParkedMessageCount

			if src.EventStreamId == "$all" {
				if src.LastCheckpointedEventPosition != "" {
					pos, err := parsePosition(src.LastCheckpointedEventPosition)

					if err != nil {
						return nil, err
					}

					stats.LastCheckpointedPosition = pos
				}

				if src.LastKnownEventPosition != "" {
					pos, err := parsePosition(src.LastKnownEventPosition)

					if err != nil {
						return nil, err
					}

					stats.LastKnownPosition = pos
				}
			} else {
				stats.LastCheckpointedEventRevision = new(uint64)
				stats.LastKnownEventRevision = new(uint64)
				*stats.LastCheckpointedEventRevision = uint64(src.LastProcessedEventNumber)
				*stats.LastKnownEventRevision = uint64(src.LastKnownEventNumber)
			}

			info.Stats = stats
		}
	}

	info.EventSource = src.EventStreamId
	info.GroupName = src.GroupName
	info.Status = src.Status
	info.Connections = src.Connections

	return &info, nil
}
