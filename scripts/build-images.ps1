# Windows PowerShell: builds all CertiFlow images with the tags used in k8s/*.yaml
Set-Location (Join-Path $PSScriptRoot "..")
foreach ($s in @("gateway","event-service","verify-service","worker")) {
  Write-Host ">> building certiflow/${s}:1.0"
  docker build -t "certiflow/${s}:1.0" "./$s"
  if ($LASTEXITCODE -ne 0) { throw "build failed for $s" }
}
Write-Host "Done."
