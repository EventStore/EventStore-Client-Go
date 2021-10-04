package event_streams_with_prepopulated_database

import "path"

const rootPath = "../../../.."

func joinRootPathAndFilePath(filePath string) string {
	return path.Join(rootPath, filePath)
}
