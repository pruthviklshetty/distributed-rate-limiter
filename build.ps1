# Windows equivalent of `make build`: build the dashboard, then the Go binary
# with web/dist embedded. Usage:  ./build.ps1  [-Run]
param([switch]$Run)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

Write-Host "==> building frontend (web/dist)" -ForegroundColor Cyan
Push-Location web
npm ci
npm run build
Pop-Location

Write-Host "==> building Go binary" -ForegroundColor Cyan
go build -o distributed-rate-limiter.exe .

Write-Host "==> done: distributed-rate-limiter.exe" -ForegroundColor Green

if ($Run) {
    Write-Host "==> starting server on :8080" -ForegroundColor Cyan
    ./distributed-rate-limiter.exe
}
