package connection_integration_test

import "path"

const rootPath = "../../../.."

func joinRootPathAndFilePath(filePath string) string {
	return path.Join(rootPath, filePath)
}
