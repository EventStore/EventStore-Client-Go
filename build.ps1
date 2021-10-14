[CmdletBinding()]
Param(
    [switch] $generateProtos
)

$ErrorActionPreference = "Stop"

function Exec {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)][scriptblock]$Command,
        [Parameter(Mandatory = $false, Position = 1)][string]$ErrorMessage = ("Failed executing {0}" -F $Command)
    )

    . $Command
    if ($LASTEXITCODE -ne 0) {
        throw ("Exec: " + $ErrorMessage)
    }
}

$protobufVersion = "3.14.0"
$protocTarball = "protoc-$protobufVersion-win64.zip"
$protocUrl = "https://github.com/protocolbuffers/protobuf/releases/download/v$protobufVersion/$protocTarball"

# Required tools
New-Item -Path . -Name "tools" -ItemType "directory" -Force | Out-Null
Push-Location tools

if (-not (Test-Path -LiteralPath "protobuf")) {
    Write-Host "Installing protoc $protobufVersion locally..."

    Invoke-WebRequest -Uri $protocUrl -OutFile $protocTarball
    Expand-Archive -LiteralPath $protocTarball -DestinationPath "protobuf" -Force
    Remove-Item $protocTarball

    Write-Host "done."
}

Pop-Location
# end

if ($generateProtos) {
    Exec { go get google.golang.org/protobuf/cmd/protoc-gen-go } "Cannot run go command"
    Exec { go get google.golang.org/grpc/cmd/protoc-gen-go-grpc } "Cannot run go command"

    # We don't check the env variables because Github actions don't set $GOPATH.
    $gopath = Exec { go env GOPATH } "Cannot get $$GOPATH"

    Write-Host "Generating gRPC code..."

    Get-ChildItem -Path "protos\*.proto" | ForEach-Object {
        $file = Get-ChildItem $_
        $baseName = $file.BaseName
        Write-Host "Compiling $baseName.proto ..."
        New-Item -Path .\protos -Name $baseName -ItemType "directory" -Force | Out-Null
        Exec { tools\protobuf\bin\protoc --proto_path=$pwd\protos --go_out=.\protos\$baseName --go-grpc_out=.\protos\$baseName --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative --plugin=protoc-gen-go=$gopath\bin\protoc-gen-go.exe --plugin=protoc-gen-go-grpc=$gopath\bin\protoc-gen-go-grpc.exe $file } "Cannot run protoc"
        Write-Host "done."
    }

    Write-Host "Code generation completed."
}

Write-Host "Compiling project..."
go build -v .\esdb .\samples
Write-Host "done."
